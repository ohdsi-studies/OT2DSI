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


.plotResults<- function(connection,
                       cdmDatabaseSchema,
                       vocabularyDatabaseSchema = cdmDatabaseSchema,
                       cohortDatabaseSchema = targetDatabaseSchema,
                       cohortTable = cohortTable,
                       oracleTempSchema,
                       output_folder = output_folder,
                       minOrdinalSize = 0.005) {

### Create output directory
outputFolder_path <- paste0(output_folder,"/pathways")

outputFolder_sunburst <- paste0(output_folder,"/sunburstPlot")
dir.create(outputFolder_sunburst, showWarnings = FALSE)

### Read csv files from package
results <- read.csv(file.path(outputFolder_path,"pathways.csv"))
cohorts <- read.csv(file.path("inst/settings","cohort_id_index_map.csv"))
all_cohorts <- read.csv(system.file("settings/CohortsToCreate.csv", package = "OT2DSI"))

results_year <- read.csv(file.path(outputFolder_path,"pathways_year.csv"))
results_age <- read.csv(file.path(outputFolder_path,"pathways_agegroup.csv"))

all_cohorts <- all_cohorts %>% mutate(atlasName = stringr::str_replace(atlasName,'OT2DSI_',''))

db_name <- stringr::str_replace_all(unique(results$dbname),"_"," ")


### Remove columns with NA in all rows
results <- results[,colSums(is.na(results)) < nrow(results)]

### Unique combo id numbers
combo_ids <- results %>% select(contains('step')) %>% gather() %>% dplyr::filter(!is.na(value)) %>% .$value %>% unique

### Unnest results data frame (via combo_ids)
combo_id_map <- tibble(combo_id = combo_ids) %>%
  mutate(eventCohortIndex = purrr::map(combo_id, .extract_bitSum)) %>%
  unnest(eventCohortIndex) %>%
  left_join(cohorts %>% select(cohort_definition_id, eventName = atlasName, eventCohortIndex = cohort_index)) %>%
  group_by(combo_id) %>%
  arrange(combo_id, cohort_definition_id) %>%
  mutate(combo_size = n(),
         combo_part = row_number(),
         min_cohort_definition_id = min(cohort_definition_id),
         eventName = as.character(eventName)) %>%
  dplyr::mutate(eventName = stringr::str_replace_all(eventName,'&','(')) %>%
  dplyr::mutate(eventName = stringr::str_replace(eventName,'%',')')) %>%
  dplyr::mutate(eventName = stringr::str_replace_all(eventName,' ',' -> ')) %>%
  dplyr::mutate(eventName = stringr::str_replace_all(eventName,'_',' ')) %>%
  arrange(combo_size, min_cohort_definition_id, combo_id, combo_part)


### Set font to avoid warning messages
#grDevices::windowsFonts(Arial=windowsFont("TT Arial"))

### Transpose results data frame
formatted_results <- results %>%
  mutate(rn = row_number()) %>%
  gather(ordinal, combo_id, -pathwayAnalysisGenerationId, -targetCohortId, -countValue, -rn, -dbname, -targetCohortCount, -pathwaysCount,-finalcombo,-cohortrate) %>%
  as_tibble


formatted_results <- formatted_results %>%
  mutate(combo_id = as.numeric(combo_id)) %>%
  left_join(combo_id_map) %>%
  select(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal,  cohort_definition_id, eventName, combo_part, combo_id, combo_size, countValue) %>%
  mutate(ordinal_numeric = as.integer(stringr::str_remove(ordinal, "step")),
         eventName = coalesce(eventName, "")) %>%
  left_join(all_cohorts %>% select(atlasName, targetCohortId = cohortId)) %>%
  filter(ordinal_numeric <= pmin(3, max(ordinal_numeric[eventName != ""]))) %>%
  split(.$atlasName, drop = T)



### Plot formatted_results data frame
plots <- map2(formatted_results, names(formatted_results),
              ~.x %>%
                ungroup %>%
                mutate(combo_part = coalesce(combo_part, 1),
                       combo_size = coalesce(combo_size, 1),
                       totalCount = sum(countValue[ordinal_numeric==1 & combo_part == 1]),
                       propOrdinal = countValue / totalCount,
                       eventName = factor(eventName, levels = c(unique(combo_id_map$eventName),""))) %>%
                filter(propOrdinal > minOrdinalSize | ordinal == "step1") %>%
                group_by(ordinal) %>%
                mutate(propOrdinal1 = case_when(combo_part==1 ~ propOrdinal, TRUE ~ 0),
                       cumProp = cumsum(propOrdinal1),
                       cumPropStart = map_dbl(rn, ~max(c(0,cumProp[rn<.x])))) %>%
                mutate(ymin = cumPropStart,
                       ymax = cumProp) %>%
                group_by(ordinal, rn) %>%
                mutate(xmin = ordinal_numeric + (combo_part/combo_size) - (1/combo_size),
                       xmax = ordinal_numeric + (combo_part/combo_size)) %>%
                select(ordinal, combo_part, eventName, rn, ymin, ymax, xmin, xmax, propOrdinal) %>%
                ggplot() +
                geom_rect(aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = eventName), size = 0) +
                scale_y_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=c(0,0)) +
                scale_x_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=expansion(add = c(1,0))) +
                scale_fill_manual(values = c(RColorBrewer::brewer.pal(length(unique(combo_id_map$eventName)), "Paired")[1:length(unique(combo_id_map$eventName))],"#ffffff"), drop=F) +
                coord_polar(theta = "y", clip="off") +
                labs(x = NULL, y = NULL, title = db_name) +
                guides(fill=guide_legend(title=NULL, ncol = 1)) +
                theme_bw(base_family="Arial",base_size=13) +
                theme(panel.ontop = TRUE,
                      legend.position = "right",
                      legend.key = element_rect(size = 3, colour = "#ffffff"),
                      legend.key.size = unit(1.5, 'lines'),
                      legend.text=element_text(size=8, vjust=1),
                      panel.grid.major = element_line(colour="#f0f0f0"),
                      panel.grid.minor = element_line(colour="#f0f0f0"),
                      panel.border = element_blank(),
                      axis.line.y = element_blank(),
                      axis.line.x = element_blank(),
                      axis.text.x =  element_blank(),
                      axis.text.y =  element_blank(),
                      panel.background = element_blank(), axis.line = element_line(colour = "black"))
)

###################################################################################################################################################################
### Transpose results data frame (YEAR)
formatted_results <- results_year %>%
  mutate(rn = row_number()) %>%
  gather(ordinal, combo_id, -pathwayAnalysisGenerationId, -targetCohortId, -countValue, -rn, -dbname, -year) %>%
  as_tibble


formatted_results <- formatted_results %>%
  mutate(combo_id = as.numeric(combo_id)) %>%
  left_join(combo_id_map) %>%
  select(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal,  cohort_definition_id, eventName, combo_part, combo_id, combo_size, countValue, year, dbname) %>%
  mutate(ordinal_numeric = as.integer(stringr::str_remove(ordinal, "step")),
         eventName = coalesce(eventName, "")) %>%
  left_join(all_cohorts %>% select(atlasName, targetCohortId = cohortId)) %>%
  filter(ordinal_numeric <= pmin(3, max(ordinal_numeric[eventName != ""]))) %>%
  split(.$atlasName, drop = T)


### Plot formatted_results data frame
################
plots_year <- map(formatted_results, ~.x %>%
  as_tibble %>%
  group_by(dbname, year) %>%
  mutate(combo_part = coalesce(combo_part, 1),
         combo_size = coalesce(combo_size, 1),
         totalCount = sum(countValue[ordinal_numeric==1 & combo_part == 1]),
         propOrdinal = countValue / totalCount,
         eventName = factor(eventName, levels = unique(c(combo_id_map$eventName,"")))) %>%
  filter(propOrdinal > minOrdinalSize | ordinal == "step1") %>%
  group_by(ordinal, dbname, year) %>%
  mutate(propOrdinal1 = case_when(combo_part==1 ~ propOrdinal, TRUE ~ 0),
         cumProp = cumsum(propOrdinal1),
         cumPropStart = map_dbl(rn, ~max(c(0,cumProp[rn<.x])))) %>%
  mutate(ymin = cumPropStart,
         ymax = cumProp) %>%
  group_by(dbname, year, ordinal, rn) %>%
  mutate(xmin = ordinal_numeric + (combo_part/combo_size) - (1/combo_size),
         xmax = ordinal_numeric + (combo_part/combo_size)) %>%
  ungroup %>%
  select(ordinal, combo_part, eventName, dbname, year, rn, ymin, ymax, xmin, xmax, propOrdinal) %>%
  ggplot() +
  geom_rect(aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = eventName), size = 0) +
  scale_y_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=c(0,0)) +
  scale_x_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=expansion(add = c(1,0))) +
  scale_fill_manual(values = c(RColorBrewer::brewer.pal(length(unique(combo_id_map$eventName)), "Paired")[1:length(unique(combo_id_map$eventName))],"#ffffff"), drop=F) +
  coord_polar(theta = "y", clip="off") +
  labs(x = NULL, y = NULL, title = db_name) +
  guides(fill=guide_legend(title=NULL, ncol = 1)) +
  theme_bw(base_family="Arial",base_size=13) +
  facet_wrap(vars(year), ncol = 4, drop=F)  +
  theme(panel.ontop = TRUE,
        legend.position = "right",
        legend.key = element_rect(size = 3, colour = "#ffffff"),
        legend.key.size = unit(1.5, 'lines'),
        legend.text=element_text(size=8, vjust=1),
        panel.grid.major = element_line(colour="#f0f0f0"),
        panel.grid.minor = element_line(colour="#f0f0f0"),
        panel.border = element_blank(),
        axis.line.y = element_blank(),
        axis.line.x = element_blank(),
        axis.text.x =  element_blank(),
        axis.text.y =  element_blank(),
        panel.background = element_blank(), axis.line = element_line(colour = "black"))
)
##################################################################################################################################################################

### Transpose results data frame (AGE)
formatted_results <- results_age %>%
  mutate(rn = row_number()) %>%
  gather(ordinal, combo_id, -pathwayAnalysisGenerationId, -targetCohortId, -countValue, -rn, -dbname, -ageGroup) %>%
  as_tibble


formatted_results <- formatted_results %>%
  mutate(combo_id = as.numeric(combo_id)) %>%
  left_join(combo_id_map) %>%
  select(pathwayAnalysisGenerationId, targetCohortId, rn, ordinal,  cohort_definition_id, eventName, combo_part, combo_id, combo_size, countValue, ageGroup, dbname) %>%
  mutate(ordinal_numeric = as.integer(stringr::str_remove(ordinal, "step")),
         eventName = coalesce(eventName, "")) %>%
  left_join(all_cohorts %>% select(atlasName, targetCohortId = cohortId)) %>%
  filter(ordinal_numeric <= pmin(3, max(ordinal_numeric[eventName != ""]))) %>%
  split(.$atlasName, drop = T)


### Plot formatted_results data frame
################
plots_age <- map(formatted_results, ~.x %>%
                    as_tibble %>%
                    mutate(ageGroup = factor(ageGroup, levels = c("Age group: 50 or younger","Age group: 50-65","Age group: 75 or older"))) %>%
                    group_by(dbname, ageGroup) %>%
                    mutate(combo_part = coalesce(combo_part, 1),
                           combo_size = coalesce(combo_size, 1),
                           totalCount = sum(countValue[ordinal_numeric==1 & combo_part == 1]),
                           propOrdinal = countValue / totalCount,
                           eventName = factor(eventName, levels = unique(c(combo_id_map$eventName,"")))) %>%
                    filter(propOrdinal > minOrdinalSize | ordinal == "step1") %>%
                    group_by(ordinal, dbname, ageGroup) %>%
                    mutate(propOrdinal1 = case_when(combo_part==1 ~ propOrdinal, TRUE ~ 0),
                           cumProp = cumsum(propOrdinal1),
                           cumPropStart = map_dbl(rn, ~max(c(0,cumProp[rn<.x])))) %>%
                    mutate(ymin = cumPropStart,
                           ymax = cumProp) %>%
                    group_by(dbname, ageGroup, ordinal, rn) %>%
                    mutate(xmin = ordinal_numeric + (combo_part/combo_size) - (1/combo_size),
                           xmax = ordinal_numeric + (combo_part/combo_size)) %>%
                    ungroup %>%
                    select(ordinal, combo_part, eventName, dbname, ageGroup, rn, ymin, ymax, xmin, xmax, propOrdinal) %>%
                    ggplot() +
                    geom_rect(aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = eventName), size = 0) +
                    scale_y_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=c(0,0)) +
                    scale_x_continuous(minor_breaks = seq(0,1,0.02), breaks = NULL, expand=expansion(add = c(1,0))) +
                    scale_fill_manual(values = c(RColorBrewer::brewer.pal(length(unique(combo_id_map$eventName)), "Paired")[1:length(unique(combo_id_map$eventName))],"#ffffff"), drop=F) +
                    coord_polar(theta = "y", clip="off") +
                    labs(x = NULL, y = NULL, title = db_name) +
                    guides(fill=guide_legend(title=NULL, ncol = 1)) +
                    theme_bw(base_family="Arial",base_size=13) +
                    facet_wrap(vars(ageGroup), ncol = 4, drop=F)  +
                    theme(panel.ontop = TRUE,
                          legend.position = "right",
                          legend.key = element_rect(size = 3, colour = "#ffffff"),
                          legend.key.size = unit(1.5, 'lines'),
                          legend.text=element_text(size=8, vjust=1),
                          panel.grid.major = element_line(colour="#f0f0f0"),
                          panel.grid.minor = element_line(colour="#f0f0f0"),
                          panel.border = element_blank(),
                          axis.line.y = element_blank(),
                          axis.line.x = element_blank(),
                          axis.text.x =  element_blank(),
                          axis.text.y =  element_blank(),
                          panel.background = element_blank(), axis.line = element_line(colour = "black"))
)
##################################################################################################################################################################


### Export data frame
# write.csv(bind_rows(formatted_results, .id="targetCohort"), file.path(outputFolder_sunburst, "formatted_pathways.csv"), row.names= FALSE)

### Export sunburst plot rds file
#saveRDS(plots, file.path(outputFolder_sunburst, "sunburstPlots.rds"))

### Export sunburst plot
walk2(names(plots), plots, ~ggsave(filename = file.path(outputFolder_sunburst, paste0("T2D_",unique(results$dbname),".png")), device = "png", plot = .y, dpi = 600, width = 8, height = 7, units = "in"))

### Export sunburst plot (Year)
walk2(names(plots_year), plots_year, ~ggsave(filename = file.path(outputFolder_sunburst, paste0("T2D_",unique(results$dbname),"_year",".png")), device = "png", plot = .y, dpi = 600, width = 8, height = 7, units = "in"))

### Export sunburst plot (Age)
walk2(names(plots_age), plots_age, ~ggsave(filename = file.path(outputFolder_sunburst, paste0("T2D_",unique(results$dbname),"_agegroup",".png")), device = "png", plot = .y, dpi = 600, width = 8, height = 7, units = "in"))

}
