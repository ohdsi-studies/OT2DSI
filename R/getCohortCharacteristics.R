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


.getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     cohortDatabaseSchema = targetDatabaseSchema,
                                     cohortTable,
                                     cohortId,
                                     minCellCount,
                                     output_folder = output_folder) {

  start <- Sys.time()

  cohorts <- read.csv(system.file("settings", "CohortsToCreate.csv", package = "OT2DSI")) %>%
    select(atlasName, cohortId) %>%
    split(.$atlasName, drop = TRUE) %>%
    purrr::map(~.x$cohortId)

  ### Create output directory
  outputFolder_char <- paste0(output_folder,"/characterization")
  dir.create(outputFolder_char, showWarnings = FALSE)

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  ### Create Feature Extraction settings
  covariateSettings_def <- FeatureExtraction::createDefaultCovariateSettings()

  ### Get covariate data
  data <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                oracleTempSchema = oracleTempSchema,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = targetDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortId = cohortId,
                                                covariateSettings = covariateSettings_def,
                                                aggregated = TRUE)

  # covariateSettings <- FeatureExtraction::createCovariateSettings(
  #   useDemographicsGender = TRUE,
  #   useDemographicsAge = TRUE,
  #   useDemographicsAgeGroup = TRUE,
  #   useDemographicsRace = TRUE,
  #   useDemographicsEthnicity = TRUE,
  #   useDemographicsIndexYear = TRUE,
  #   useDemographicsIndexMonth = TRUE,
  #   useDemographicsPriorObservationTime = TRUE,
  #   useDemographicsPostObservationTime = TRUE,
  #   useDemographicsTimeInCohort = TRUE,
  #   useConditionEraLongTerm = TRUE,
  #   useDrugEraLongTerm = TRUE,
  #   useDrugGroupEraLongTerm = TRUE,
  #   useDistinctIngredientCountLongTerm = TRUE,
  #   useDrugEraShortTerm = TRUE,
  #   useDrugGroupEraShortTerm = TRUE,
  #   useDistinctIngredientCountShortTerm = TRUE,
  #   useProcedureOccurrenceLongTerm = TRUE,
  #   useDeviceExposureLongTerm = TRUE,
  #   useMeasurementLongTerm = TRUE,
  #   useVisitCountLongTerm = TRUE,
  #   useVisitConceptCountLongTerm = TRUE,
  #   useCharlsonIndex = TRUE,
  #   useDcsi = TRUE,
  #   useChads2 = TRUE,
  #   useChads2Vasc = TRUE,
  #   useHfrs = TRUE,
  #   longTermStartDays = -365,
  #   shortTermStartDays = -30,
  #   endDays = -1
  # )


  if (!is.null(data$covariates)) {
    n <- attr(x = data, which = "metaData")$populationSize
    if (FeatureExtraction::isTemporalCovariateData(data)) {
      counts <- data$covariates %>%
        dplyr::collect() %>%
        dplyr::mutate(sd = sqrt(((n * .data$sumValue) + .data$sumValue)/(n^2)))


      binaryCovs <- data$covariates %>%
        dplyr::select(.data$timeId, .data$covariateId, .data$averageValue) %>%
        dplyr::rename(mean = .data$averageValue) %>%
        dplyr::collect() %>%
        dplyr::left_join(counts, by = c("covariateId", "timeId")) %>%
        dplyr::select(-.data$averageValue)
    } else {
      counts <- data$covariates %>%
        dplyr::collect() %>%
        dplyr::mutate(sd = sqrt(((n * .data$sumValue) + .data$sumValue)/(n^2)))

      binaryCovs <- data$covariates %>%
        dplyr::select(.data$covariateId, .data$averageValue) %>%
        dplyr::rename(mean = .data$averageValue) %>%
        dplyr::collect() %>%
        dplyr::left_join(counts, by = "covariateId") %>%
        dplyr::select(-.data$averageValue)
    }

    if (nrow(binaryCovs) > 0) {
      if (FeatureExtraction::isTemporalCovariateData(data)) {
        binaryCovs <- binaryCovs %>%
          dplyr::left_join(y = data$timeRef %>% dplyr::collect(), by = "timeId") %>%
          dplyr::rename(startDayTemporalCharacterization = .data$startDay,
                        endDayTemporalCharacterization = .data$endDay) %>%
          mutate(case_when(mean <= minCellCount/attr(data, "metaData")$populationSize ~ -minCellCount/attr(data, "metaData")$populationSize,
                           TRUE ~ mean))
      }
    }
  }else{
    binaryCovs <- NULL}

  # Note that for some covariates (such as the Charlson comorbidity index) a value of 0 is interpreted as the value 0, while for
  # other covariates (Such as blood pressure) 0 is interpreted as missing, and the distribution statistics are only computed over non-missing values.
  # To learn which continuous covariates fall into which category one can consult the missingMeansZero field in the covariateData$analysisRef object.


  if (!is.null(data$covariatesContinuous)) {
    if (FeatureExtraction::isTemporalCovariateData(data)) {
      continuousCovs <- data$covariatesContinuous %>%
        dplyr::select(.data$timeId, .data$countValue, .data$covariateId, .data$averageValue, .data$standardDeviation, .data$minValue, .data$maxValue, .data$p10Value, .data$p25Value, .data$medianValue, .data$p75Value, .data$p90Value) %>%
        dplyr::rename(sd = .data$standardDeviation) %>%
        dplyr::collect() %>%
        dplyr::mutate(mean = .data$countValue /  attr(data, "metaData")$populationSize)
    } else {
      continuousCovs <- data$covariatesContinuous %>%
        dplyr::select(.data$countValue, .data$covariateId, .data$averageValue, .data$standardDeviation, .data$minValue, .data$maxValue, .data$p10Value, .data$p25Value, .data$medianValue, .data$p75Value, .data$p90Value) %>%
        dplyr::rename(sd = .data$standardDeviation) %>%
        dplyr::collect() %>%
        dplyr::mutate(mean = .data$countValue /  attr(data, "metaData")$populationSize)
    }

    if (nrow(continuousCovs) > 0) {
      if (FeatureExtraction::isTemporalCovariateData(data)) {
        continuousCovs <- continuousCovs %>%
          dplyr::left_join(y = data$timeRef %>% dplyr::collect(), by = "timeId") %>%
          dplyr::rename(startDayTemporalCharacterization = .data$startDay,
                        endDayTemporalCharacterization = .data$endDay)  %>%
          filter(countValue >= pmax(minCellCount, 100))
      }
    }

  }else{
    continuousCovs <- NULL
  }

  delta <- Sys.time() - start
  if (FeatureExtraction::isTemporalCovariateData(data)) {
    ParallelLogger::logInfo(paste("Temporal Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  } else {
    ParallelLogger::logInfo(paste("Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }



  continuousCovs <- continuousCovs %>%
    dplyr::select(everything())%>%
    dplyr::left_join(y = data$covariateRef %>% collect(), by = "covariateId") %>%
    dplyr::select(-.data$conceptId, -.data$mean) %>%
    dplyr::mutate(dbname = outputFolder) %>%
    dplyr::mutate(averageValue = round(averageValue,2)) %>%
    dplyr::mutate(sd = round(sd,2)) %>%
    dplyr::rename(mean = averageValue)


  binaryCovs <- binaryCovs %>%
    dplyr::select(everything())%>%
    dplyr::left_join(y = data$covariateRef %>% collect(), by = "covariateId") %>%
    dplyr::filter(sumValue >=5) %>%
    dplyr::mutate(dbname = outputFolder) %>%
    dplyr::mutate(mean = mean * 100) %>%
    dplyr::mutate(mean = round(mean,2)) %>%
    dplyr::mutate(sd = round(sd,2)) %>%
    dplyr::rename(`proportion (%)` = mean)


### Table 1 file (report)
table1 <- FeatureExtraction::createTable1(data, showCounts = T)
write.csv(table1, file.path(outputFolder_char , "table1.csv"), row.names= F)

### Categorical covariates file (Annex)
write.csv(binaryCovs, file.path(outputFolder_char , "categorical_covariates.csv"), row.names= F)

### Continuous covariates file (Annex)
write.csv(continuousCovs, file.path(outputFolder_char , "continuous_covariates.csv"), row.names= F)

}
