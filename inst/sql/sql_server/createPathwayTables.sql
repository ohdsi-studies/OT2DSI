IF OBJECT_ID('@target_database_schema.@pathway_analysis_events', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@pathway_analysis_events;

CREATE TABLE @target_database_schema.@pathway_analysis_events (
  pathway_analysis_generation_id INT,
  target_cohort_id INT,
  subject_id BIGINT,
  ordinal INT,
  combo_id BIGINT,
  cohort_start_date DATE,
  cohort_end_date DATE
);

IF OBJECT_ID('@target_database_schema.@pathway_analysis_stats', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@pathway_analysis_stats;

CREATE TABLE @target_database_schema.@pathway_analysis_stats (
  pathway_analysis_generation_id INT,
  target_cohort_id INT,
  target_cohort_count BIGINT,
  pathways_count BIGINT
);

IF OBJECT_ID('@target_database_schema.@pathway_analysis_paths', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@pathway_analysis_paths;

CREATE TABLE @target_database_schema.@pathway_analysis_paths (
  pathway_analysis_generation_id INT,
  target_cohort_id INT,
  step_1 BIGINT,
  step_2 BIGINT,
  step_3 BIGINT,
  step_4 BIGINT,
  step_5 BIGINT,
  step_6 BIGINT,
  step_7 BIGINT,
  step_8 BIGINT,
  step_9 BIGINT,
  step_10 BIGINT,
  count_value BIGINT
);


-- Create new table for age group and year stratas
IF OBJECT_ID('@target_database_schema.@pathway_analysis_paths', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@pathway_analysis_paths_2;

CREATE TABLE @target_database_schema.@pathway_analysis_paths_2 (
  pathway_analysis_generation_id INT,
  target_cohort_id INT,
  year INT,
  age_group VARCHAR(100),
  step_1 BIGINT,
  step_2 BIGINT,
  step_3 BIGINT,
  step_4 BIGINT,
  step_5 BIGINT,
  step_6 BIGINT,
  step_7 BIGINT,
  step_8 BIGINT,
  step_9 BIGINT,
  step_10 BIGINT,
  count_value BIGINT
);
