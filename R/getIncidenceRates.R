# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of OT2DSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


.getIncidenceRates <- function (connectionDetails = NULL,
                                connection = NULL,
                                cohortDatabaseSchema = targetDatabaseSchema,
                                cdmDatabaseSchema,
                                oracleTempSchema = NULL,
                                cohortTable,
                                output_folder = output_folder) {

### Create output directory
outputFolder_ir <- paste0(output_folder,"/incidenceRates")
dir.create(outputFolder_ir, showWarnings = FALSE)

### Create result tables
sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "createIncidenceRatesTables.sql",
                                           packageName = "OT2DSI",
                                           dbms = attr(connection, "dbms"),
                                           target_database_schema = targetDatabaseSchema,
                                           ir_analysis_dist = paste0(cohortTable, "_ir_analysis_dist"),
                                           ir_analysis_result = paste0(cohortTable, "_ir_analysis_result"),
                                           ir_analysis_strata_stats = paste0(cohortTable, "_ir_analysis_strata_stats"),
                                           ir_strata = paste0(cohortTable, "_ir_strata"))

DatabaseConnector::executeSql(connection, sql)

### Insert cohortInserts
cohortInserts <- read.csv(system.file("settings", "cohortInserts.csv", package = "OT2DSI"))
DatabaseConnector::insertTable(connection, "#cohortInserts", cohortInserts, tempTable = TRUE)
cohortInserts <- "SELECT * from #cohortInserts"

### Read CohortToCreate
pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "OT2DSI")
cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())

### Set variables
adjustedStart <- "dateadd(day ,-1 ,cohort_start_date)" ### Set to -1 to include patients with outcome on index date
adjustedEnd <- "cohort_end_date"
cdm_database_schema <- cdmDatabaseSchema

### Run Incidence Rates Analysis
sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "runIncidenceRatesAnalysis.sql",
                                         packageName = "OT2DSI",
                                         dbms = attr(connection, "dbms"),
                                         ir_analysis_dist = paste0(cohortTable, "_ir_analysis_dist"),
                                         ir_analysis_dist_tto = paste0(cohortTable, "_ir_analysis_dist_tto"),
                                         ir_analysis_result = paste0(cohortTable, "_ir_analysis_result"),
                                         ir_analysis_strata_stats = paste0(cohortTable, "_ir_analysis_strata_stats"),
                                         ir_strata = paste0(cohortTable, "_ir_strata"),
                                         cohortInserts = cohortInserts,
                                         adjustedStart = adjustedStart,
                                         adjustedEnd = adjustedEnd,
                                         cohort_table = cohortTable,
                                         cdm_database_schema = cdm_database_schema,
                                         temp_database_schema = targetDatabaseSchema,
                                         results_database_schema = targetDatabaseSchema,
                                         analysisId = 1,
                                         cohortDataFilter = "",
                                         EndDateUnions = "",
                                         codesetQuery = "",
                                         strataCohortInserts = "")


DatabaseConnector::executeSql(connection, sql)

### TTO file
sql <- "SELECT * FROM @cohort_database_schema.@cohort_table"

sql <- SqlRender::render(sql,
                         cohort_database_schema = targetDatabaseSchema,
                         cohort_table = paste0(cohortTable, "_ir_analysis_dist_tto"))

sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))

ir_results <- DatabaseConnector::querySql(connection, sql)

names(ir_results) <- SqlRender::snakeCaseToCamelCase(names(ir_results))

ir_results<-ir_results %>%
            dplyr::left_join(cohortsToCreate, by = c("outcomeId" = "cohortId"))  %>%
            dplyr::mutate(name = stringr::str_replace(name,'OT2DSI_','')) %>%
            dplyr::mutate(name = stringr::str_replace_all(name,'&','(')) %>%
            dplyr::mutate(name = stringr::str_replace(name,'%',')')) %>%
            dplyr::mutate(name = stringr::str_replace_all(name,'_',' '))
            #dplyr::mutate(dbname = outputFolder)


ir_results<-ir_results %>%
  dplyr::select(-outcomeId,-analysisId,-targetId,-strataSequence,-atlasName,-atlasId,-cohortType,-distType) %>%
  dplyr::rename('Median' = medianValue, 'p5' = p5Value, 'p10'= p10Value,, 'p25'= p25Value,, 'p75'= p75Value,, 'p90'= p90Value, 'p95'= p95Value, 'min'= minValue, 'max'= maxValue, 'Outcome'= name,'SD'= stdDev, 'Mean'= avgValue, 'Patient Count' = total)

ir_results_all <- ir_results %>%
  dplyr::filter(strataName == 'All') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)

ir_results_age<- ir_results %>%
  dplyr::filter(strataName == '50-75' | strataName == 'Above 75' | strataName == 'Below 50') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)

ir_results_year <- ir_results %>%
  dplyr::filter(strataName != '50-75' & strataName != 'Above 75' & strataName != 'Below 50' & strataName != 'All') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)


write.csv(ir_results, file.path(outputFolder_ir, "ir_results_tto.csv"), row.names= FALSE)
write.csv(ir_results_all, file.path(outputFolder_ir, "ir_results_tto_all.csv"), row.names= FALSE)
write.csv(ir_results_age, file.path(outputFolder_ir, "ir_results_tto_age.csv"), row.names= FALSE)
write.csv(ir_results_year, file.path(outputFolder_ir, "ir_results_tto_year.csv"), row.names= FALSE)
write.csv(ir_results, file.path(outputFolder_ir, "ir_results_tto.csv"), row.names= FALSE)

### TAR file
sql <- "SELECT * FROM @cohort_database_schema.@cohort_table"

sql <- SqlRender::render(sql,
                         cohort_database_schema = targetDatabaseSchema,
                         cohort_table = paste0(cohortTable, "_ir_analysis_dist"))


sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))

ir_results <- DatabaseConnector::querySql(connection, sql)

names(ir_results) <- SqlRender::snakeCaseToCamelCase(names(ir_results))

ir_results<-ir_results %>%
  dplyr::left_join(cohortsToCreate, by = c("outcomeId" = "cohortId"))  %>%
  dplyr::mutate(name = stringr::str_replace(name,'OT2DSI_','')) %>%
  dplyr::mutate(name = stringr::str_replace_all(name,'&','(')) %>%
  dplyr::mutate(name = stringr::str_replace(name,'%',')')) %>%
  dplyr::mutate(name = stringr::str_replace_all(name,'_',' '))
  #dplyr::mutate(dbname = outputFolder)

ir_results<-ir_results %>%
  dplyr::select(-outcomeId,-analysisId,-targetId,-strataSequence,-atlasName,-atlasId,-cohortType,-distType) %>%
  dplyr::rename('Median' = medianValue, 'p5' = p5Value, 'p10'= p10Value,, 'p25'= p25Value,, 'p75'= p75Value,, 'p90'= p90Value, 'p95'= p95Value, 'min'= minValue, 'max'= maxValue, 'Outcome'= name,'SD'= stdDev, 'Mean'= avgValue, 'Patient Count' = total)

ir_results_all <- ir_results %>%
  dplyr::filter(strataName == 'All') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)

ir_results_age<- ir_results %>%
  dplyr::filter(strataName == '50-75' | strataName == 'Above 75' | strataName == 'Below 50') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)

ir_results_year <- ir_results %>%
  dplyr::filter(strataName != '50-75' & strataName != 'Above 75' & strataName != 'Below 50' & strataName != 'All') %>%
  dplyr::select('Outcome', everything()) %>%
  dplyr::arrange(Outcome,strataName)


write.csv(ir_results, file.path(outputFolder_ir, "ir_results_tar.csv"), row.names= FALSE)
write.csv(ir_results_all, file.path(outputFolder_ir, "ir_results_tar_all.csv"), row.names= FALSE)
write.csv(ir_results_age, file.path(outputFolder_ir, "ir_results_tar_age.csv"), row.names= FALSE)
write.csv(ir_results_year, file.path(outputFolder_ir, "ir_results_tar_year.csv"), row.names= FALSE)


### Main file
sql <- "
      SELECT
        *,
        rate * 1000 as rate_per_1k,
        proportion * 1000 as proportion_per_1k
      from
        (
          SELECT
          *,
          ROUND(CAST((cast(cases as float)/nullif(cast(time_at_risk as float),0)) AS FLOAT),5) as rate,
  	      ROUND(CAST((cast(cases as float)/nullif(cast(time_at_risk as float),0)) -  1.96 *((cast(cases as float)/nullif(cast(time_at_risk as float),0))/nullif(sqrt(cases),0)) AS FLOAT),5) * 1000 as ir_interval_left,
  	      ROUND(CAST((cast(cases as float)/nullif(cast(time_at_risk as float),0)) +  1.96 *((cast(cases as float)/nullif(cast(time_at_risk as float),0))/nullif(sqrt(cases),0)) AS FLOAT),5) * 1000 as ir_interval_right,
  	      ROUND(CAST((cast(cases as float)/nullif(cast(person_count as float),0)) AS FLOAT),3) as proportion,
  		  ROUND(CAST((cast(cases as float)/nullif(cast(person_count as float),0)) -  1.96 * sqrt((cast(cases as float)/nullif(cast(person_count as float),0)) * (1-(cast(cases as float)/nullif(cast(person_count as float),0)))/nullif(cases,0)) AS FLOAT),3) * 1000  as prop_interval_left,
  		  ROUND(CAST(proportion +  1.96 * sqrt(proportion * (1-proportion)/nullif(cases,0)) AS FLOAT),3) * 1000  as prop_interval_right
          FROM @cohort_database_schema.@cohort_table
                                                );"

sql <- SqlRender::render(sql,
                         cohort_database_schema = targetDatabaseSchema,
                         cohort_table = paste0(cohortTable, "_ir_analysis_strata_stats"))


sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))

ir_results <- DatabaseConnector::querySql(connection, sql)

names(ir_results) <- SqlRender::snakeCaseToCamelCase(names(ir_results))

ir_results<-ir_results %>%
  dplyr::left_join(cohortsToCreate, by = c("outcomeId" = "cohortId"))  %>%
  dplyr::mutate(name = stringr::str_replace(name,'OT2DSI_','')) %>%
  dplyr::mutate(name = stringr::str_replace_all(name,'&','(')) %>%
  dplyr::mutate(name = stringr::str_replace(name,'%',')')) %>%
  dplyr::mutate(name = stringr::str_replace_all(name,'_',' ')) %>%
  dplyr::mutate(dbname = outputFolder)

ir_results<-ir_results %>%
  dplyr::select(-outcomeId,-analysisId,-targetId, -strataSequence, -atlasName,-atlasId,-cohortType)

ir_results <- ir_results %>%
  dplyr::mutate(irIntervalLeft= dplyr::if_else(irIntervalLeft < 0,0,irIntervalLeft)) %>%
  dplyr::mutate(propIntervalLeft= dplyr::if_else(propIntervalLeft < 0,0,propIntervalLeft)) %>%
  dplyr::mutate(ir_ci = paste0(ratePer1k,' (',irIntervalLeft,' , ',irIntervalRight,')')) %>%
  dplyr::mutate(prop_ci = paste0(proportionPer1k,' (',propIntervalLeft,' , ',propIntervalRight,')'))

ir_results<-ir_results %>%
  dplyr::select(-irIntervalLeft, -irIntervalRight, -rate, -proportion, -propIntervalLeft, -propIntervalRight, -dbname, -ratePer1k, -proportionPer1k) %>%
  dplyr::rename('Patient Count' = personCount, 'Time at Risk (years)' = timeAtRisk, 'Cases' = cases, 'Rate per 1k years' = ir_ci, 'Proportion per 1k persons' = prop_ci, 'Outcome' =name)

ir_results<-ir_results %>%
  dplyr::select('Outcome','strataName', 'Patient Count', 'Cases','Proportion per 1k persons', 'Time at Risk (years)','Rate per 1k years')

ir_results_all <- ir_results %>%
  dplyr::filter(strataName == 'All') %>%
  dplyr::arrange(Outcome)

ir_results_age<- ir_results %>%
  dplyr::filter(strataName == '50-75' | strataName == 'Above 75' | strataName == 'Below 50') %>%
  dplyr::arrange(Outcome)

ir_results_year <- ir_results %>%
  dplyr::filter(strataName != '50-75' & strataName != 'Above 75' & strataName != 'Below 50' & strataName != 'All') %>%
  dplyr::arrange(Outcome)

write.csv(ir_results_all, file.path(outputFolder_ir, "ir_stats_all.csv"), row.names= FALSE)
write.csv(ir_results_age, file.path(outputFolder_ir, "ir_stats_age.csv"), row.names= FALSE)
write.csv(ir_results_year, file.path(outputFolder_ir, "ir_stats_year.csv"), row.names= FALSE)

}
