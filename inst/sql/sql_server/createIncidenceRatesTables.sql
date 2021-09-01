IF OBJECT_ID('@target_database_schema.@ir_analysis_dist', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@ir_analysis_dist;

CREATE TABLE @target_database_schema.@ir_analysis_dist
(
	analysis_id INT,
	target_id INT,
	outcome_id INT,
	strata_sequence INT,
	strata_name VARCHAR(500),
	dist_type VARCHAR(255),
	total BIGINT,
	avg_value DOUBLE PRECISION,
	std_dev DOUBLE PRECISION,
	min_value INT,
	p5_value INT,
	p10_value INT,
	p25_value INT,
	median_value INT,
	p75_value INT,
	p90_value INT,
	p95_value INT,
	max_value INT
);


IF OBJECT_ID('@target_database_schema.@ir_analysis_dist_tto', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@ir_analysis_dist_tto;

CREATE TABLE @target_database_schema.@ir_analysis_dist_tto
(
	analysis_id INT,
	target_id INT,
	outcome_id INT,
	strata_sequence INT,
	strata_name VARCHAR(500),
	dist_type VARCHAR(255),
	total BIGINT,
	avg_value DOUBLE PRECISION,
	std_dev DOUBLE PRECISION,
	min_value INT,
	p5_value INT,
	p10_value INT,
	p25_value INT,
	median_value INT,
	p75_value INT,
	p90_value INT,
	p95_value INT,
	max_value INT
);


IF OBJECT_ID('@target_database_schema.@ir_analysis_result', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@ir_analysis_result;

CREATE TABLE @target_database_schema.@ir_analysis_result
(
	analysis_id INT,
	target_id INT,
	outcome_id INT,
	strata_mask BIGINT,
	person_count BIGINT,
	time_at_risk BIGINT,
	dist_type VARCHAR(255),
	cases BIGINT
);




IF OBJECT_ID('@target_database_schema.@ir_analysis_strata_stats', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@ir_analysis_strata_stats;

CREATE TABLE @target_database_schema.@ir_analysis_strata_stats
(
	analysis_id INT,
	target_id INT,
	outcome_id INT,
	strata_sequence INT,
	person_count BIGINT,
	time_at_risk BIGINT,
	cases BIGINT,
	strata_name VARCHAR(255)
);



IF OBJECT_ID('@target_database_schema.@ir_strata', 'U') IS NOT NULL
DROP TABLE @target_database_schema.@ir_strata;

CREATE TABLE @target_database_schema.@ir_strata
(
	analysis_id INT,
	strata_sequence INT,
	name VARCHAR(255),
	description VARCHAR(1000)
);





