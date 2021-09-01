INSERT INTO @target_database_schema.@pathway_analysis_paths (pathway_analysis_generation_id, target_cohort_id, step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10, count_value)
select pathway_analysis_generation_id, target_cohort_id,
	step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10,
  count_big(subject_id) as count_value
from
(
  select e.pathway_analysis_generation_id, e.target_cohort_id, e.subject_id,
    MAX(case when ordinal = 1 then combo_id end) as step_1,
    MAX(case when ordinal = 2 then combo_id end) as step_2,
    MAX(case when ordinal = 3 then combo_id end) as step_3,
    MAX(case when ordinal = 4 then combo_id end) as step_4,
    MAX(case when ordinal = 5 then combo_id end) as step_5,
    MAX(case when ordinal = 6 then combo_id end) as step_6,
    MAX(case when ordinal = 7 then combo_id end) as step_7,
    MAX(case when ordinal = 8 then combo_id end) as step_8,
    MAX(case when ordinal = 9 then combo_id end) as step_9,
    MAX(case when ordinal = 10 then combo_id end) as step_10
  from @target_database_schema.@pathway_analysis_events e
  WHERE e.pathway_analysis_generation_id = @generation_id
	GROUP BY e.pathway_analysis_generation_id, e.target_cohort_id, e.subject_id
) t1
group by pathway_analysis_generation_id, target_cohort_id,
	step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Additional table with age group and year stratification

INSERT INTO @target_database_schema.@pathway_analysis_paths_2 (pathway_analysis_generation_id, target_cohort_id, year, age_group, step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10, count_value)
select pathway_analysis_generation_id, target_cohort_id, year, age_group,
	step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10,
  count_big(subject_id) as count_value
from
 (
	select	t1.pathway_analysis_generation_id, t1.target_cohort_id, t1.subject_id, max(t1.year) as year, max(t1.age_group) as age_group,
	    MAX(case when ordinal = 1 then combo_id end) as step_1,
	    MAX(case when ordinal = 2 then combo_id end) as step_2,
	    MAX(case when ordinal = 3 then combo_id end) as step_3,
	    MAX(case when ordinal = 4 then combo_id end) as step_4,
	    MAX(case when ordinal = 5 then combo_id end) as step_5,
	    MAX(case when ordinal = 6 then combo_id end) as step_6,
	    MAX(case when ordinal = 7 then combo_id end) as step_7,
	    MAX(case when ordinal = 8 then combo_id end) as step_8,
	    MAX(case when ordinal = 9 then combo_id end) as step_9,
	    MAX(case when ordinal = 10 then combo_id end) as step_10
	from
    (
	select
		  e.*,
	   	p.year_of_birth,
		  datepart(year,cohort_start_date) as year,
		  datepart(year,cohort_start_date) - p.year_of_birth as age,
		  case when age<50 then 'Age group: 50 or younger'
			     when age>=50 and age<=75 then 'Age group: 50-75'
			     when age>75 then 'Age group: 75 or older' end as age_group
	from @target_database_schema.@pathway_analysis_events e
	left join @cdmDatabaseSchema.person p on p.person_id = e.subject_id
	WHERE e.pathway_analysis_generation_id = @generation_id and age_group is not null
	) t1
 GROUP BY t1.pathway_analysis_generation_id, t1.target_cohort_id, t1.subject_id
) t2
group by pathway_analysis_generation_id, target_cohort_id, year, age_group,
	step_1, step_2, step_3, step_4, step_5, step_6, step_7, step_8, step_9, step_10
;
