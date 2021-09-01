-- SQL file that creates all combinations of possible paths.
-- We will use the result of this script to join with the final pathways data frame.

select
	id_combos,
	combo_no,
	combo_name
INTO #comboPathName
from (

-- P(7,1) = 7!/(7-1)! = 7
select
	cast(c1.powered_index as varchar(5)) as id_combos,
	c1.powered_index as combo_no,
	c1.atlasName as combo_name
from #cohort_id_index_map c1

UNION

-- P(7,2) = 7!/(7-2)! = 42
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index as combo_no,
	c1.atlasName +'+' + c2.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index

UNION

-- P(7,3) = ...
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) +'+' + cast(c3.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index + c3.powered_index as combo_no,
    c1.atlasName +'+' + c2.atlasName +'+' + c3.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index
inner join #cohort_id_index_map c3 on c2.powered_index <c3.powered_index

UNION

-- P(7,4) = ...
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) +'+' + cast(c3.powered_index as varchar(5)) +'+' + cast(c4.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index + c3.powered_index + c4.powered_index as combo_no,
	c1.atlasName +'+' + c2.atlasName +'+' + c3.atlasName +'+' + c4.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index
inner join #cohort_id_index_map c3 on c2.powered_index <c3.powered_index
inner join #cohort_id_index_map c4 on c3.powered_index <c4.powered_index

UNION

-- P(7,5) = ...
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) +'+' + cast(c3.powered_index as varchar(5)) +'+' + cast(c4.powered_index as varchar(5)) +'+' + cast(c5.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index + c3.powered_index + c4.powered_index + c5.powered_index as combo_no,
	c1.atlasName +'+' + c2.atlasName +'+' + c3.atlasName +'+' + c4.atlasName +'+' + c5.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index
inner join #cohort_id_index_map c3 on c2.powered_index <c3.powered_index
inner join #cohort_id_index_map c4 on c3.powered_index <c4.powered_index
inner join #cohort_id_index_map c5 on c4.powered_index <c5.powered_index

UNION

-- P(7,6) = ...
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) +'+' + cast(c3.powered_index as varchar(5)) +'+' + cast(c4.powered_index as varchar(5)) +'+' + cast(c5.powered_index as varchar(5)) +'+' + cast(c6.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index + c3.powered_index + c4.powered_index + c5.powered_index + c6.powered_index as combo_no,
	c1.atlasName +'+' + c2.atlasName +'+' + c3.atlasName +'+' + c4.atlasName +'+' + c5.atlasName +'+' + c6.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index
inner join #cohort_id_index_map c3 on c2.powered_index <c3.powered_index
inner join #cohort_id_index_map c4 on c3.powered_index <c4.powered_index
inner join #cohort_id_index_map c5 on c4.powered_index <c5.powered_index
inner join #cohort_id_index_map c6 on c5.powered_index <c6.powered_index

UNION

-- P(7,7) = ...
select
	cast(c1.powered_index as varchar(5)) +'+' + cast(c2.powered_index as varchar(5)) +'+' + cast(c3.powered_index as varchar(5)) +'+' + cast(c4.powered_index as varchar(5)) +'+' + cast(c5.powered_index as varchar(5)) +'+' + cast(c6.powered_index as varchar(5)) +'+' + cast(c7.powered_index as varchar(5)) as id_combos,
	c1.powered_index + c2.powered_index + c3.powered_index + c4.powered_index + c5.powered_index + c6.powered_index + c7.powered_index as combo_no,
	c1.atlasName +'+' + c2.atlasName +'+' + c3.atlasName +'+' + c4.atlasName +'+' + c5.atlasName +'+' + c6.atlasName +'+' + c7.atlasName as combo_name
from #cohort_id_index_map c1
inner join #cohort_id_index_map c2 on c1.powered_index <c2.powered_index
inner join #cohort_id_index_map c3 on c2.powered_index <c3.powered_index
inner join #cohort_id_index_map c4 on c3.powered_index <c4.powered_index
inner join #cohort_id_index_map c5 on c4.powered_index <c5.powered_index
inner join #cohort_id_index_map c6 on c5.powered_index <c6.powered_index
inner join #cohort_id_index_map c7 on c6.powered_index <c7.powered_index

)
;
