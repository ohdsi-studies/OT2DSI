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



.generatePathways <- function(connection,
                        targetDatabaseSchema = targetDatabaseSchema,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        cohortTable = cohortTable,
                        event_cohort_id_index_map,
                        target_cohorts,
                        pathwayDef,
                        oracleTempSchema,
                        output_folder = output_folder) {


DatabaseConnector::insertTable(connection, "#cohort_id_index_map", event_cohort_id_index_map, tempTable = TRUE)

### Create directory
outputFolder_path <- paste0(output_folder,"/pathways")
dir.create(outputFolder_path, showWarnings = FALSE)


########################################################################
### Create combination path names
sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename =  "comboPathName.sql",
    packageName = "OT2DSI",
    dbms = attr(connection, "dbms"))

DatabaseConnector::executeSql(connection, sql)

sql <- SqlRender::render("SELECT * FROM #comboPathName;")

comboPathName <- DatabaseConnector::querySql(connection, sql)

colnames(comboPathName) <- SqlRender::snakeCaseToCamelCase(colnames(comboPathName))


### Create result tables
sql <- SqlRender::loadRenderTranslateSql(
  sqlFilename =  "createPathwayTables.sql",
  packageName = "OT2DSI",
  dbms = attr(connection, "dbms"),
  target_database_schema = targetDatabaseSchema,
  pathway_analysis_events = paste0(cohortTable, "_pathway_analysis_events"),
  pathway_analysis_stats = paste0(cohortTable, "_pathway_analysis_stats"),
  pathway_analysis_paths = paste0(cohortTable, "_pathway_analysis_paths"))

DatabaseConnector::executeSql(connection, sql)


### Run pathway analysis
for(i in target_cohorts$cohort_definition_id){

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename =  "runPathwayAnalysis.sql",
    packageName = "OT2DSI",
    dbms = attr(connection, "dbms"),
    target_database_schema = targetDatabaseSchema,
    pathway_analysis_events = paste0(cohortTable, "_pathway_analysis_events"),
    pathway_analysis_stats = paste0(cohortTable, "_pathway_analysis_stats"),
    target_cohort_table = cohortTable,
    event_cohort_id_index_map = "#cohort_id_index_map",
    combo_window = pathwayDef$combinationWindow,
    allow_repeats = dplyr::case_when(pathwayDef$allowRepeats ~ "'true'", TRUE ~ "'false'"),
    generation_id = 1,
    pathway_target_cohort_id = i,
    max_depth = pathwayDef$maxDepth)

  DatabaseConnector::executeSql(connection, sql)
}

### Save results
sql <- SqlRender::loadRenderTranslateSql(
  sqlFilename =  "savePaths.sql",
  packageName = "OT2DSI",
  dbms = attr(connection, "dbms"),
  target_database_schema = targetDatabaseSchema,
  cdmDatabaseSchema      = cdmDatabaseSchema,
  pathway_analysis_events = paste0(cohortTable, "_pathway_analysis_events"),
  pathway_analysis_paths = paste0(cohortTable, "_pathway_analysis_paths"),
  pathway_analysis_paths_2 = paste0(cohortTable, "_pathway_analysis_paths_2"),
  generation_id = 1)

DatabaseConnector::executeSql(connection, sql)
########################################################################


########################################################################
### Pathway_analysis_stats
sql <- SqlRender::render("SELECT * FROM @target_database_schema.@pathway_analysis_stats;",
                         target_database_schema = targetDatabaseSchema,
                         pathway_analysis_stats = paste0(cohortTable, "_pathway_analysis_stats"))

stats <- DatabaseConnector::querySql(connection, sql)

colnames(stats) <- SqlRender::snakeCaseToCamelCase(colnames(stats))

stats <- stats %>% dplyr::select(-pathwayAnalysisGenerationId)
########################################################################


########################################################################
### Select pathway_analysis_paths_2 table results
sql <- SqlRender::render("SELECT * FROM @target_database_schema.@pathway_analysis_paths;",
                         target_database_schema = targetDatabaseSchema,
                         pathway_analysis_paths = paste0(cohortTable, "_pathway_analysis_paths_2"))

pathways_2 <- DatabaseConnector::querySql(connection, sql)

colnames(pathways_2) <- SqlRender::snakeCaseToCamelCase(colnames(pathways_2))

gathered_results_2 <- pathways_2 %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(rn = row_number()) %>%
  gather(ordinal, combo_id, -pathwayAnalysisGenerationId, -targetCohortId, -countValue, -rn, -ageGroup, -year)
########################################################################


########################################################################
limited_pathways_year_agegroup <- gathered_results_2 %>%
  mutate(ordinal_numeric = factor(ordinal, levels = str_c("step",c(1:10)), ordered = T)) %>%
  arrange(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal_numeric) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  mutate(so_far = map_chr(ordinal_numeric, ~str_c(combo_id[ordinal_numeric <= .x], collapse="_"))) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, so_far) %>%
  mutate(combo_id = ifelse(sum(countValue) < pathwayDef$minCellCount, NA, combo_id)) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  filter(sum(!is.na(combo_id))>0) %>%
  mutate(final_pathway = str_c(combo_id[!is.na(combo_id)], collapse="_")) %>%
  filter(!is.na(final_pathway) & final_pathway != "") %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, ordinal_numeric, combo_id, final_pathway, year, ageGroup) %>%
  summarise(countValue = sum(countValue)) %>%
  ungroup %>%
  spread(ordinal_numeric, combo_id) %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(dbname = outputFolder) %>%
  select(-final_pathway)

### Filter counts below 5
limited_pathways_year_agegroup<- limited_pathways_year_agegroup %>% dplyr::filter(countValue >= 5)

### limited pathways stratified by age group and year
limited_pathways_year_agegroup <- limited_pathways_year_agegroup %>%
  left_join(comboPathName,by=(c("step1"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step2"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step3"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step4"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step5"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step6"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step7"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step8"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step9"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step10"="comboNo"))) %>%
  left_join(stats,by="targetCohortId")

### Add database name and cohort rate columns
limited_pathways_year_agegroup <- limited_pathways_year_agegroup %>%
  dplyr::mutate(dbname = outputFolder) %>%
  dplyr::mutate(cohortrate = paste((round((as.numeric(countValue)/ as.numeric(targetCohortCount))*100,digits = 3)),'%'))

### Convert all columns to character
limited_pathways_year_agegroup <- as.data.frame(apply(limited_pathways_year_agegroup ,2,as.character),stringsAsFactors = FALSE)

### Replace NA with null character
limited_pathways_year_agegroup [is.na(limited_pathways_year_agegroup )] <- ""

### Sort out string finalcombo columnn
limited_pathways_year_agegroup <- limited_pathways_year_agegroup %>%
  dplyr::mutate(finalcombo = paste(comboName.x,comboName.y,comboName.x.x,comboName.y.y,comboName.x.x.x,comboName.y.y.y,comboName.x.x.x.x,comboName.y.y.y.y,comboName.x.x.x.x.x,comboName.y.y.y.y.y)) %>%
  dplyr::mutate(finalcombo = str_squish(finalcombo)) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'&','(')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace(finalcombo,'%',')')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,' ',' -> ')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'_',' '))

limited_pathways_year_agegroup <- limited_pathways_year_agegroup %>% dplyr::select(-contains(c("comboName","idCombos")))
########################################################################

########################################################################
limited_pathways_year <- gathered_results_2 %>%
  mutate(ordinal_numeric = factor(ordinal, levels = str_c("step",c(1:10)), ordered = T)) %>%
  arrange(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal_numeric) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  mutate(so_far = map_chr(ordinal_numeric, ~str_c(combo_id[ordinal_numeric <= .x], collapse="_"))) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, so_far) %>%
  mutate(combo_id = ifelse(sum(countValue) < pathwayDef$minCellCount, NA, combo_id)) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  filter(sum(!is.na(combo_id))>0) %>%
  mutate(final_pathway = str_c(combo_id[!is.na(combo_id)], collapse="_")) %>%
  filter(!is.na(final_pathway) & final_pathway != "") %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, ordinal_numeric, combo_id, final_pathway, year) %>%
  summarise(countValue = sum(countValue)) %>%
  ungroup %>%
  spread(ordinal_numeric, combo_id) %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(dbname = outputFolder) %>%
  select(-final_pathway)


limited_pathways_year <- limited_pathways_year %>% dplyr::filter(countValue >= 5)

### limited pathways stratified by age group and year
limited_pathways_year <- limited_pathways_year %>%
  left_join(comboPathName,by=(c("step1"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step2"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step3"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step4"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step5"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step6"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step7"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step8"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step9"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step10"="comboNo"))) %>%
  left_join(stats,by="targetCohortId")

### Add database name and cohort rate columns
limited_pathways_year <- limited_pathways_year%>%
  dplyr::mutate(dbname = outputFolder) %>%
  dplyr::mutate(cohortrate = paste((round((as.numeric(countValue)/ as.numeric(targetCohortCount))*100,digits = 3)),'%'))

### Convert all columns to character
limited_pathways_year<- as.data.frame(apply(limited_pathways_year ,2,as.character),stringsAsFactors = FALSE)

### Replace NA with null character
limited_pathways_year [is.na(limited_pathways_year)] <- ""

### Sort out string finalcombo columnn
limited_pathways_year <- limited_pathways_year %>%
  dplyr::mutate(finalcombo = paste(comboName.x,comboName.y,comboName.x.x,comboName.y.y,comboName.x.x.x,comboName.y.y.y,comboName.x.x.x.x,comboName.y.y.y.y,comboName.x.x.x.x.x,comboName.y.y.y.y.y)) %>%
  dplyr::mutate(finalcombo = str_squish(finalcombo)) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'&','(')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace(finalcombo,'%',')')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,' ',' -> ')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'_',' '))

limited_pathways_year <- limited_pathways_year %>% dplyr::select(-contains(c("comboName","idCombos")))
########################################################################


########################################################################
limited_pathways_agegroup <- gathered_results_2 %>%
  mutate(ordinal_numeric = factor(ordinal, levels = str_c("step",c(1:10)), ordered = T)) %>%
  arrange(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal_numeric) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  mutate(so_far = map_chr(ordinal_numeric, ~str_c(combo_id[ordinal_numeric <= .x], collapse="_"))) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, so_far) %>%
  mutate(combo_id = ifelse(sum(countValue) < pathwayDef$minCellCount, NA, combo_id)) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  filter(sum(!is.na(combo_id))>0) %>%
  mutate(final_pathway = str_c(combo_id[!is.na(combo_id)], collapse="_")) %>%
  filter(!is.na(final_pathway) & final_pathway != "") %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, ordinal_numeric, combo_id, final_pathway, ageGroup) %>%
  summarise(countValue = sum(countValue)) %>%
  ungroup %>%
  spread(ordinal_numeric, combo_id) %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(dbname = outputFolder) %>%
  select(-final_pathway)

limited_pathways_agegroup <- limited_pathways_agegroup %>% dplyr::filter(countValue >= 5)

### limited pathways stratified by age group and year
limited_pathways_agegroup<- limited_pathways_agegroup %>%
  left_join(comboPathName,by=(c("step1"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step2"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step3"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step4"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step5"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step6"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step7"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step8"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step9"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step10"="comboNo"))) %>%
  left_join(stats,by="targetCohortId")

### Add database name and cohort rate columns
limited_pathways_agegroup<- limited_pathways_agegroup %>%
  dplyr::mutate(dbname = outputFolder) %>%
  dplyr::mutate(cohortrate = paste((round((as.numeric(countValue)/ as.numeric(targetCohortCount))*100,digits = 3)),'%'))

### Convert all columns to character
limited_pathways_agegroup <- as.data.frame(apply(limited_pathways_agegroup ,2,as.character),stringsAsFactors = FALSE)

### Replace NA with null character
limited_pathways_agegroup[is.na(limited_pathways_agegroup)] <- ""

### Sort out string finalcombo columnn
limited_pathways_agegroup <- limited_pathways_agegroup %>%
  dplyr::mutate(finalcombo = paste(comboName.x,comboName.y,comboName.x.x,comboName.y.y,comboName.x.x.x,comboName.y.y.y,comboName.x.x.x.x,comboName.y.y.y.y,comboName.x.x.x.x.x,comboName.y.y.y.y.y)) %>%
  dplyr::mutate(finalcombo = str_squish(finalcombo)) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'&','(')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace(finalcombo,'%',')')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,' ',' -> ')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'_',' '))

limited_pathways_agegroup <- limited_pathways_agegroup %>% dplyr::select(-contains(c("comboName","idCombos")))
########################################################################


########################################################################
### Select pathway_analysis_paths table results
sql <- SqlRender::render("SELECT * FROM @target_database_schema.@pathway_analysis_paths;",
                         target_database_schema = targetDatabaseSchema,
                         pathway_analysis_paths = paste0(cohortTable, "_pathway_analysis_paths"))

pathways <- DatabaseConnector::querySql(connection, sql)

colnames(pathways) <- SqlRender::snakeCaseToCamelCase(colnames(pathways))

gathered_results <- pathways %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(rn = row_number()) %>%
  gather(ordinal, combo_id, -pathwayAnalysisGenerationId, -targetCohortId, -countValue, -rn)
########################################################################


########################################################################
limited_pathways <- gathered_results %>%
  mutate(ordinal_numeric = factor(ordinal, levels = str_c("step",c(1:10)), ordered = T)) %>%
  arrange(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal_numeric) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  mutate(so_far = map_chr(ordinal_numeric, ~str_c(combo_id[ordinal_numeric <= .x], collapse="_"))) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, so_far) %>%
  mutate(combo_id = ifelse(sum(countValue) < pathwayDef$minCellCount, NA, combo_id)) %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, rn) %>%
  filter(sum(!is.na(combo_id))>0) %>%
  mutate(final_pathway = str_c(combo_id[!is.na(combo_id)], collapse="_")) %>%
  filter(!is.na(final_pathway) & final_pathway != "") %>%
  group_by(pathwayAnalysisGenerationId, targetCohortId, ordinal_numeric, combo_id, final_pathway) %>%
  summarise(countValue = sum(countValue)) %>%
  ungroup %>%
  spread(ordinal_numeric, combo_id) %>%
  arrange(step1, step2, step3, step4, step5, step6, step7, step8, step9, step10) %>%
  mutate(dbname = outputFolder) %>%
  select(-final_pathway)


### Original limited pathways
limited_pathways <- limited_pathways %>%
  left_join(comboPathName,by=(c("step1"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step2"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step3"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step4"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step5"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step6"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step7"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step8"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step9"="comboNo"))) %>%
  left_join(comboPathName,by=(c("step10"="comboNo"))) %>%
  left_join(stats,by="targetCohortId")

limited_pathways <- limited_pathways %>%
  dplyr::mutate(dbname = outputFolder) %>%
  dplyr::mutate(cohortrate = paste((round((as.numeric(countValue)/ as.numeric(targetCohortCount))*100,digits = 3)),'%'))

### Convert all columns to character
limited_pathways <- as.data.frame(apply(limited_pathways,2,as.character),stringsAsFactors = FALSE)

### Replace NA with null character
limited_pathways[is.na(limited_pathways)] <- ""

limited_pathways <- limited_pathways %>%
  dplyr::mutate(finalcombo = paste(comboName.x,comboName.y,comboName.x.x,comboName.y.y,comboName.x.x.x,comboName.y.y.y,comboName.x.x.x.x,comboName.y.y.y.y,comboName.x.x.x.x.x,comboName.y.y.y.y.y)) %>%
  dplyr::mutate(finalcombo = str_squish(finalcombo)) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'&','(')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace(finalcombo,'%',')')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,' ',' -> ')) %>%
  dplyr::mutate(finalcombo = stringr::str_replace_all(finalcombo,'_',' '))

limited_pathways <- limited_pathways %>% dplyr::select(-contains(c("comboName","idCombos")))

### Create top 20 pathways data frame
top20pathways <- limited_pathways %>% select(dbname, finalcombo, countValue, cohortrate) %>% arrange(desc(countValue)) %>% top_n(20)
########################################################################


########################################################################
### Export stats file
write.csv(stats, file.path(outputFolder_path, "stats.csv"), row.names= F)

### Export top 20 pathways file (Report)
write.csv(top20pathways, file.path(outputFolder_path, "top20pathways.csv"), row.names = F)

### Export original limited pathways data frame (Annex)
write.csv(limited_pathways, file.path(outputFolder_path, "pathways.csv"), row.names = F)

### Export limited pathways data frame with year and age group (Annex)
write.csv(limited_pathways_year_agegroup , file.path(outputFolder_path, "pathways_year_agegroup.csv"), row.names= F)

### Export limited pathways data frame with year (Annex)
write.csv(limited_pathways_year, file.path(outputFolder_path, "pathways_year.csv"), row.names= F)

### Export limited pathways data frame with age group (Annex)
write.csv(limited_pathways_agegroup, file.path(outputFolder_path, "pathways_agegroup.csv"), row.names= F)
########################################################################

}



