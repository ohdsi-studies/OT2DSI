

select cohort_id, is_outcome
into #cohorts
FROM (
  @cohortInserts
) C
;

with cteCohortCombos (target_id, outcome_id) as
(
  select t.cohort_id as target_id, o.cohort_id as outcome_id
  FROM #cohorts t
  CROSS JOIN #cohorts o
  where t.is_outcome = 0 and o.is_outcome = 1
),
cteCohortData(target_id, outcome_id, subject_id, cohort_start_date, cohort_end_date, adjusted_start_date, adjusted_end_date, op_start_date, op_end_date) as
(
  select combos.target_id, combos.outcome_id, t.subject_id, t.cohort_start_date, t.cohort_end_date, t.adjusted_start_date, t.adjusted_end_date, op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date
  from cteCohortCombos combos
  join (
    select cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, @adjustedStart as adjusted_start_date, @adjustedEnd as adjusted_end_date
    FROM @temp_database_schema.@cohort_table
  ) t on t.cohort_definition_id = combos.target_id
  join @cdm_database_schema.observation_period op on t.subject_id = op.person_id and t.cohort_start_date between op.observation_period_start_date and op.observation_period_end_date
  left join (
    select cohort_definition_id, subject_id, min(cohort_start_date) as cohort_start_date
    from @temp_database_schema.@cohort_table
    GROUP BY cohort_definition_id, subject_id
  ) O on o.cohort_definition_id = combos.outcome_id
  and t.subject_id = o.subject_id
  where (o.cohort_start_date is null or o.cohort_start_date > t.adjusted_start_date)
  and t.adjusted_start_date < t.adjusted_end_date
  and t.adjusted_start_date between op.observation_period_start_date and op.observation_period_end_date
  @cohortDataFilter
),
cteEndDates (target_id, outcome_id, subject_id, cohort_start_date, followup_end, is_case) as
(
  select target_id, outcome_id, subject_id, cohort_start_date, followup_end, is_case
  FROM (
    select target_id, outcome_id, subject_id, cohort_start_date, followup_end, is_case, row_number() over (partition by target_id, outcome_id, subject_id, cohort_start_date order by followup_end asc, is_case desc) as RN
    FROM (
      select combos.target_id, combos.outcome_id, t.subject_id, t.cohort_start_date, t.op_end_date as followup_end, 0 as is_case
      from cteCohortCombos combos
      join cteCohortData t on combos.target_id = t.target_id and combos.outcome_id = t.outcome_id

      UNION
      select combos.target_id, combos.outcome_id, t.subject_id, t.cohort_start_date, t.adjusted_end_date as followup_end, 0 as is_case
      from cteCohortCombos combos
      join cteCohortData t on combos.target_id = t.target_id and combos.outcome_id = t.outcome_id

      UNION
      select combos.target_id, combos.outcome_id, t.subject_id, t.cohort_start_date, o.cohort_start_date as followup_end, 1 as is_case
      from cteCohortCombos combos
      join cteCohortData t on combos.target_id = t.target_id and combos.outcome_id = t.outcome_id
      join (
        select cohort_definition_id, subject_id, min(cohort_start_date) as cohort_start_date
        from @temp_database_schema.@cohort_table
        GROUP BY cohort_definition_id, subject_id
      ) O on o.cohort_definition_id = combos.outcome_id and t.subject_id = o.subject_id
      where o.cohort_start_date > t.adjusted_start_date

      @EndDateUnions

    ) RawData
  ) Result
  WHERE Result.RN = 1
),
cteRawData (target_id, outcome_id, subject_id, cohort_start_date, cohort_end_date, time_at_risk, is_case) as
(
  select t.target_id, t.outcome_id, t.subject_id, t.cohort_start_date, t.cohort_end_date, datediff(d,t.adjusted_start_date, e.followup_end) as time_at_risk, e.is_case
  from cteCohortData t
  join cteEndDates e on t.target_id = e.target_id
  and t.outcome_id = e.outcome_id
  and t.subject_id = e.subject_id
  and t.cohort_start_date = e.cohort_start_date
)
select target_id, outcome_id, subject_id, cohort_start_date, cohort_end_date, time_at_risk, is_case
INTO #time_at_risk
from cteRawData
;

-- from here, take all the people's person_id, start_date, end_date, create an 'events table'
CREATE TABLE #analysis_events (
  event_id BIGINT,
  person_id BIGINT,
  start_date DATE,
  end_date DATE,
  op_start_date DATE,
  op_end_date DATE,
  TARGET_CONCEPT_ID BIGINT,
  visit_occurrence_id BIGINT
);

INSERT INTO #analysis_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, TARGET_CONCEPT_ID, visit_occurrence_id)
select row_number() over (partition by P.person_id order by P.start_date) as event_id, P.person_id, P.start_date, P.end_date, P.op_start_date, P.op_end_date, CAST(NULL as bigint) as TARGET_CONCEPT_ID, CAST(NULL as bigint) as visit_occurrence_id
FROM
(
  select distinct T.subject_id as person_id, T.cohort_start_date as start_date, T.cohort_end_date as end_date, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
  from #time_at_risk T
  JOIN @cdm_database_schema.observation_period OP on T.subject_id = OP.person_id and T.cohort_start_date between OP.observation_period_start_date and OP.observation_period_end_date
) P
;

-- create the stratifiction set
@codesetQuery

create table #strataCohorts
(
  strata_sequence int,
  person_id bigint,
  event_id bigint
)
;

-------------------------------------------------------------------------------------------------

truncate table @results_database_schema.@ir_strata;

----------------------------------------------------------------------------------------
-- Insert strata sequences
INSERT INTO @results_database_schema.@ir_strata (analysis_id, strata_sequence, name)
select analysis_id, strata_sequence, name
from
	(
		select 1 as analysis_id, 24 as strata_sequence, 'All' as name
		UNION ALL
		select 1 as analysis_id, 1 as strata_sequence, 'Below 50' as name
		UNION ALL
		select 1 as analysis_id, 2 as strata_sequence, '50-75' as name
		UNION ALL
		select 1 as analysis_id, 3 as strata_sequence, 'Above 75' as name
		UNION ALL
		select 1 as analysis_id, 4 as strata_sequence, '2001' as name
		UNION ALL
		select 1 as analysis_id, 5 as strata_sequence, '2002' as name
		UNION ALL
		select 1 as analysis_id, 6 as strata_sequence, '2003' as name
		UNION ALL
		select 1 as analysis_id, 7 as strata_sequence, '2004' as name
		UNION ALL
		select 1 as analysis_id, 8 as strata_sequence, '2005' as name
		UNION ALL
		select 1 as analysis_id, 9 as strata_sequence, '2006' as name
		UNION ALL
		select 1 as analysis_id, 10 as strata_sequence, '2007' as name
		UNION ALL
		select 1 as analysis_id, 11 as strata_sequence, '2008' as name
		UNION ALL
		select 1 as analysis_id, 12 as strata_sequence, '2009' as name
		UNION ALL
		select 1 as analysis_id, 13 as strata_sequence, '2010' as name
		UNION ALL
		select 1 as analysis_id, 14 as strata_sequence, '2011' as name
		UNION ALL
		select 1 as analysis_id, 15 as strata_sequence, '2012' as name
		UNION ALL
		select 1 as analysis_id, 16 as strata_sequence, '2013' as name
		UNION ALL
		select 1 as analysis_id, 17 as strata_sequence, '2014' as name
		UNION ALL
		select 1 as analysis_id, 18 as strata_sequence, '2015' as name
		UNION ALL
		select 1 as analysis_id, 19 as strata_sequence, '2016' as name
		UNION ALL
		select 1 as analysis_id, 20 as strata_sequence, '2017' as name
		UNION ALL
		select 1 as analysis_id, 21 as strata_sequence, '2018' as name
		UNION ALL
		select 1 as analysis_id, 22 as strata_sequence, '2019' as name

	)
;

--@strataCohortInserts
INSERT INTO #strataCohorts (strata_sequence ,person_id ,event_id)
select strata_sequence ,person_id ,event_id
from
	(
		select
			ae.person_id,
			ae.event_id,
			24 as strata_sequence
		from #analysis_events ae

		UNION

		select
			p.person_id,
			ae.event_id,
			CASE WHEN  datepart(year,ae.start_date) - p.year_of_birth < 50 then 1
				 when    datepart(year,ae.start_date) - p.year_of_birth >=50 and datepart(year,ae.start_date) - p.year_of_birth<=75 then  2
				 when    datepart(year,ae.start_date) - p.year_of_birth > 75	then 3 end
			as strata_sequence
		from #analysis_events ae
		join @cdm_database_schema.person p on ae.person_id = p.person_id


		UNION

		select
			ae.person_id,
			ae.event_id,
			CASE WHEN  datepart(year,ae.start_date) = 2001 then 4
				 when  datepart(year,ae.start_date) = 2002 then 5
				 when  datepart(year,ae.start_date) = 2003 then 6
				 when  datepart(year,ae.start_date) = 2004 then 7
				 when  datepart(year,ae.start_date) = 2005 then 8
				 when  datepart(year,ae.start_date) = 2006 then 9
				 when  datepart(year,ae.start_date) = 2007 then 10
				 when  datepart(year,ae.start_date) = 2008 then 11
				 when  datepart(year,ae.start_date) = 2009 then 12
				 when  datepart(year,ae.start_date) = 2010 then 13
				 when  datepart(year,ae.start_date) = 2011 then 14
				 when  datepart(year,ae.start_date) = 2012 then 15
				 when  datepart(year,ae.start_date) = 2013 then 16
				 when  datepart(year,ae.start_date) = 2014 then 17
				 when  datepart(year,ae.start_date) = 2015 then 18
				 when  datepart(year,ae.start_date) = 2016 then 19
				 when  datepart(year,ae.start_date) = 2017 then 20
				 when  datepart(year,ae.start_date) = 2018 then 21
				 when  datepart(year,ae.start_date) = 2019 then 22 end
				  as strata_sequence
			--5 + GREATEST(0, floor((datepart(year,ae.start_date)-2001))) as strata_sequence   --- make 5 year age buckets... <1995, 1995-1999, 2000-2004 etc
		from #analysis_events ae
	)
;

-- join back the followup to the stratification and write to the results page.

DELETE FROM @results_database_schema.@ir_analysis_result where analysis_id = @analysisId;

INSERT INTO @results_database_schema.@ir_analysis_result (analysis_id, target_id, outcome_id, strata_mask, person_count, time_at_risk, dist_type, cases)
select @analysisId as analysis_id, T.target_id, T.outcome_id, CAST(E.strata_mask AS bigint),
  COUNT(subject_id) as person_count,
  CAST(ROUND(sum(1.0 * time_at_risk / 365.25),0) AS BIGINT) as time_at_risk,
  'TAR' as dist_type,
  sum(is_case) as cases
from #time_at_risk T
JOIN (
  select E.event_id, E.person_id, E.start_date, E.end_date, SUM(coalesce(POWER(cast(2 as bigint), SC.strata_sequence), 0)) as strata_mask
  FROM #analysis_events E
  LEFT JOIN #strataCohorts SC on SC.person_id = E.person_id and SC.event_id = E.event_id
  group by E.event_id, E.person_id, E.start_date, E.end_date
) E on T.subject_id = E.person_id and T.cohort_start_date = E.start_date and T.cohort_end_date = E.end_date
GROUP BY T.target_id, T.outcome_id, E.strata_mask, is_case
;

----------------------------------------------------------------------------------------------------

-- note in the case of no stratification (no rows in strataCohorts temp table), everyone will have strata_mask of 0.

-- calculate the individual strata counts from the raw person data. Rows from #strataCohorts are used to find counts for each strata

delete from @results_database_schema.@ir_analysis_strata_stats where analysis_id = @analysisId;

insert into @results_database_schema.@ir_analysis_strata_stats (analysis_id, target_id, outcome_id, strata_sequence, person_count, time_at_risk, cases, strata_name)
select irs.analysis_id, combos.target_id, combos.outcome_id, irs.strata_sequence, coalesce(T.person_count, 0) as person_count, CAST(coalesce(T.time_at_risk, 0) AS bigint) as time_at_risk, coalesce(T.cases, 0) as cases,
    case when irs.strata_sequence = 24 then 'All'
  	 when irs.strata_sequence = 1 then 'Below 50'
  	 when irs.strata_sequence = 2 then '50-75'
	   when irs.strata_sequence = 3 then 'Above 75'
	   when irs.strata_sequence = 4 then '2001'
	   when irs.strata_sequence = 5 then '2002'
	   when irs.strata_sequence = 6 then '2003'
	   when irs.strata_sequence = 7 then '2004'
	   when irs.strata_sequence = 8 then '2005'
	   when irs.strata_sequence = 9 then '2006'
	   when irs.strata_sequence = 10 then '2007'
	   when irs.strata_sequence = 11 then '2008'
	   when irs.strata_sequence = 12 then '2009'
	   when irs.strata_sequence = 13 then '2010'
	   when irs.strata_sequence = 14 then '2011'
	   when irs.strata_sequence = 15 then '2012'
	   when irs.strata_sequence = 16 then '2013'
	   when irs.strata_sequence = 17 then '2014'
	   when irs.strata_sequence = 18 then '2015'
	   when irs.strata_sequence = 19 then '2016'
	   when irs.strata_sequence = 20 then '2017'
	   when irs.strata_sequence = 21 then '2018'
	   when irs.strata_sequence = 22 then '2019'
	   else
  NULL end as strata_name
from @results_database_schema.@ir_strata irs
cross join (
  select t.cohort_id as target_id, o.cohort_id as outcome_id
  FROM #cohorts t
  CROSS JOIN #cohorts o
  where t.is_outcome = 0 and o.is_outcome = 1
) combos
left join
(
  select T.target_id, T.outcome_id, S.strata_sequence, count(S.event_id) as person_count, sum(1.0 * T.time_at_risk / 365.25) as time_at_risk, sum(T.is_case) as cases
  from #analysis_events E
  JOIN #strataCohorts S on S.person_id = E.person_id and E.event_id = S.event_id
  join #time_at_risk T on T.subject_id = E.person_id and T.cohort_start_date = E.start_date and T.cohort_end_date = E.end_date
  group by T.target_id, T.outcome_id, S.strata_sequence
) T on irs.strata_sequence = T.strata_sequence and T.target_id = combos.target_id and T.outcome_id = combos.outcome_id
WHERE irs.analysis_id = @analysisId
;

-- calculate distributions for TAR and TTO by strata

-- dist_type 1: time at risk

WITH cteRawData (target_id, outcome_id, strata_sequence, count_value) as
(
  select T.target_id, T.outcome_id, 24 as strata_sequence, T.time_at_risk as count_value
  from #time_at_risk T

  UNION ALL

  select T.target_id, T.outcome_id, S.strata_sequence, T.time_at_risk as count_value
  from #analysis_events E
  JOIN #strataCohorts S on E.person_id = S.person_id and E.event_id = S.event_id
  join #time_at_risk T on T.subject_id = E.person_id and T.cohort_start_date = E.start_date and T.cohort_end_date = E.end_date
)
, overallStats (target_id, outcome_id, strata_sequence, avg_value, stdev_value, min_value, max_value, total) as
(
  select target_id,
    outcome_id,
    strata_sequence,
    avg(1.000 * count_value) as avg_value,
    stdev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
  from cteRawData
  group by target_id, outcome_id, strata_sequence
),
stats (target_id, outcome_id, strata_sequence, count_value, total, rn) as
(
  select target_id, outcome_id, strata_sequence, count_value, count_big(*) as total, row_number() over (partition by target_id, outcome_id, strata_sequence order by count_value) as rn
  FROM cteRawData
  group by target_id, outcome_id, strata_sequence, count_value
),
priorStats (target_id, outcome_id, strata_sequence, count_value, total, accumulated) as
(
  select s.target_id, s.outcome_id, s.strata_sequence, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.target_id = p.target_id and s.outcome_id = p.outcome_id and s.strata_sequence = p.strata_sequence and p.rn <= s.rn
  group by s.target_id, s.outcome_id, s.strata_sequence, s.count_value, s.total, s.rn
)
select
  o.target_id,
  o.outcome_id,
  o.strata_sequence,
  o.total,
  CAST(o.avg_value AS FLOAT) as avg_value,
  CAST(coalesce(o.stdev_value, 0.0) AS FLOAT) as stdev_value,
  o.min_value,
  MIN(case when p.accumulated >= .05 * o.total then count_value else o.max_value end) as p5_value,
  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value,
  MIN(case when p.accumulated >= .95 * o.total then count_value else o.max_value end) as p95_value,
  o.max_value
INTO #tempTARDist
from priorStats p
join overallStats o on p.target_id = o.target_id and p.outcome_id = o.outcome_id and p.strata_sequence = o.strata_sequence
GROUP BY o.target_id, o.outcome_id, o.strata_sequence, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

DELETE FROM @results_database_schema.@ir_analysis_dist where analysis_id = @analysisId;

INSERT INTO @results_database_schema.@ir_analysis_dist (analysis_id, dist_type, target_id, outcome_id, strata_sequence, strata_name, total, avg_value, std_dev, min_value, p5_value, p10_value, p25_value, median_value, p75_value, p90_value, p95_value, max_value)
select @analysisId as analysis_id, 'TAR' as dist_type, combos.target_id, combos.outcome_id,
    strata_sequence,
    case when d.strata_sequence = 24 then 'All'
  	 when d.strata_sequence = 1 then 'Below 50'
  	 when d.strata_sequence = 2 then '50-75'
	   when d.strata_sequence = 3 then 'Above 75'
	   when d.strata_sequence = 4 then '2001'
	   when d.strata_sequence = 5 then '2002'
	   when d.strata_sequence = 6 then '2003'
	   when d.strata_sequence = 7 then '2004'
	   when d.strata_sequence = 8 then '2005'
	   when d.strata_sequence = 9 then '2006'
	   when d.strata_sequence = 10 then '2007'
	   when d.strata_sequence = 11 then '2008'
	   when d.strata_sequence = 12 then '2009'
	   when d.strata_sequence = 13 then '2010'
	   when d.strata_sequence = 14 then '2011'
	   when d.strata_sequence = 15 then '2012'
	   when d.strata_sequence = 16 then '2013'
	   when d.strata_sequence = 17 then '2014'
	   when d.strata_sequence = 18 then '2015'
	   when d.strata_sequence = 19 then '2016'
	   when d.strata_sequence = 20 then '2017'
	   when d.strata_sequence = 21 then '2018'
	   when d.strata_sequence = 22 then '2019'
	   else
  NULL end as strata_name,
  d.total, round(d.avg_value,0), round(d.stdev_value,0), d.min_value, d.p5_value, d.p10_value, d.p25_value, d.median_value, d.p75_value, d.p90_value, d.p95_value, d.max_value
FROM
(
  select t.cohort_id as target_id, o.cohort_id as outcome_id
  FROM #cohorts t
  CROSS JOIN #cohorts o
  where t.is_outcome = 0 and o.is_outcome = 1
) combos
JOIN #tempTARDist d on combos.target_id = d.target_id and combos.outcome_id = d.outcome_id
;

TRUNCATE TABLE #tempTARDist;
DROP TABLE #tempTARDist;


-- dist_type 2: TTO (time to outcome)

WITH cteRawData (target_id, outcome_id, strata_sequence, count_value) as
(
  select T.target_id, T.outcome_id, 24 as strata_sequence, T.time_at_risk as count_value
  from #time_at_risk T
  where T.is_case = 1

  UNION ALL

  select T.target_id, T.outcome_id, S.strata_sequence, T.time_at_risk as count_value
  from #analysis_events E
  JOIN #strataCohorts S on E.person_id = S.person_id and E.event_id = S.event_id
  join #time_at_risk T on T.subject_id = E.person_id and T.cohort_start_date = E.start_date and T.cohort_end_date = E.end_date
  where T.is_case = 1
)
, overallStats (target_id, outcome_id, strata_sequence, avg_value, stdev_value, min_value, max_value, total) as
(
  select target_id,
    outcome_id,
    strata_sequence,
    avg(1.000 * count_value) as avg_value,
    stdev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
  from cteRawData
  group by target_id, outcome_id, strata_sequence
),
stats (target_id, outcome_id, strata_sequence, count_value, total, rn) as
(
  select target_id, outcome_id, strata_sequence, count_value, count_big(*) as total, row_number() over (partition by target_id, outcome_id, strata_sequence order by count_value) as rn
  FROM cteRawData
  group by target_id, outcome_id, strata_sequence, count_value
),
priorStats (target_id, outcome_id, strata_sequence, count_value, total, accumulated) as
(
  select s.target_id, s.outcome_id, s.strata_sequence, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.target_id = p.target_id and s.outcome_id = p.outcome_id and s.strata_sequence = p.strata_sequence and p.rn <= s.rn
  group by s.target_id, s.outcome_id, s.strata_sequence, s.count_value, s.total, s.rn
)
select
  o.target_id,
  o.outcome_id,
  o.strata_sequence,
  o.total,
  CAST(o.avg_value AS FLOAT) as avg_value,
  CAST(coalesce(o.stdev_value, 0.0) AS FLOAT) as stdev_value,
  o.min_value,
  MIN(case when p.accumulated >= .05 * o.total then count_value else o.max_value end) as p5_value,
  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value,
  MIN(case when p.accumulated >= .95 * o.total then count_value else o.max_value end) as p95_value,
  o.max_value
INTO #tempTTODist
from priorStats p
join overallStats o on p.target_id = o.target_id and p.outcome_id = o.outcome_id and p.strata_sequence = o.strata_sequence
GROUP BY o.target_id, o.outcome_id, o.strata_sequence, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

DELETE FROM @results_database_schema.@ir_analysis_dist_tto where analysis_id = @analysisId;

INSERT INTO @results_database_schema.@ir_analysis_dist_tto (analysis_id, dist_type, target_id, outcome_id, strata_sequence, strata_name, total, avg_value, std_dev,min_value, p5_value, p10_value, p25_value, median_value, p75_value, p90_value, p95_value, max_value)
select @analysisId as analysis_id, 'TTO' as dist_type, combos.target_id, combos.outcome_id,
    strata_sequence,
    case when d.strata_sequence = 24 then 'All'
  	 when d.strata_sequence = 1 then 'Below 50'
  	 when d.strata_sequence = 2 then '50-75'
	   when d.strata_sequence = 3 then 'Above 75'
	   when d.strata_sequence = 4 then '2001'
	   when d.strata_sequence = 5 then '2002'
	   when d.strata_sequence = 6 then '2003'
	   when d.strata_sequence = 7 then '2004'
	   when d.strata_sequence = 8 then '2005'
	   when d.strata_sequence = 9 then '2006'
	   when d.strata_sequence = 10 then '2007'
	   when d.strata_sequence = 11 then '2008'
	   when d.strata_sequence = 12 then '2009'
	   when d.strata_sequence = 13 then '2010'
	   when d.strata_sequence = 14 then '2011'
	   when d.strata_sequence = 15 then '2012'
	   when d.strata_sequence = 16 then '2013'
	   when d.strata_sequence = 17 then '2014'
	   when d.strata_sequence = 18 then '2015'
	   when d.strata_sequence = 19 then '2016'
	   when d.strata_sequence = 20 then '2017'
	   when d.strata_sequence = 21 then '2018'
	   when d.strata_sequence = 22 then '2019'
	   else
  NULL end as strata_name,
  d.total, round(d.avg_value,0), round(d.stdev_value,0), d.min_value, d.p5_value, d.p10_value, d.p25_value, d.median_value, d.p75_value, d.p90_value, d.p95_value,  d.max_value
FROM
(
  select t.cohort_id as target_id, o.cohort_id as outcome_id
  FROM #cohorts t
  CROSS JOIN #cohorts o
  where t.is_outcome = 0 and o.is_outcome = 1
) combos
JOIN #tempTTODist d on combos.target_id = d.target_id and combos.outcome_id = d.outcome_id
;


TRUNCATE TABLE #tempTTODist;
DROP TABLE #tempTTODist;

--TRUNCATE TABLE #Codesets;
--DROP TABLE #Codesets;

TRUNCATE TABLE #strataCohorts;
DROP TABLE #strataCohorts;

TRUNCATE TABLE #cohorts;
DROP TABLE #cohorts;

TRUNCATE TABLE #time_at_risk;
DROP TABLE #time_at_risk;

TRUNCATE TABLE #analysis_events;
DROP TABLE #analysis_events;
