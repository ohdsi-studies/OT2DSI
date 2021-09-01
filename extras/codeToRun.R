### Load libraries
library(OT2DSI)


### Connection details to connect to SQL server
connectionDetails <-  DatabaseConnector::createConnectionDetails(
  dbms = "",
  server = "",
  user = "",
  password = "" ,
  port = 5439)

### Declare variables
cdmDatabaseSchema <- ""                                             ### Name of the schema where the CDM data are located
targetDatabaseSchema <- ""                           				### Name of the schema where the results of the package will be saved
cohortTable <- ""                                                   ### Name of the table where the target and outcome cohort data will be saved
oracleTempSchema <- NULL                                            ### (Only for Oracle users)Name of temp schema
outputFolder <- ""                                        			### Name of the folder the outputs will be saved (it should named after the database)


### Connect to the server
con <- DatabaseConnector::connect(connectionDetails)

### Execute analysis
OT2DSI::execute(connection = con,
                cdmDatabaseSchema = cdmDatabaseSchema,
                targetDatabaseSchema = targetDatabaseSchema,
                oracleTempSchema = oracleTempSchema,
                cohortTable = cohortTable,
                outputFolder = outputFolder,
                createCohorts = T,
                generatePathways = T,
                plotResults = T,
                getCharacterization = T,
                getIncidenceRates = T
)
