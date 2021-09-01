OT2DSI: Observational study of type 2 diabetes and its complications: a chronological overview using the OHDSI network
=============

<img src="https://img.shields.io/badge/Study%20Status-Design%20Finalized-brightgreen.svg" alt="Study Status: Design Finalized">

- Analytics use case(s): **Characterization**
- Study type: **Clinical application**
- Tags: **Type 2 Diabetes, Pathways, Characterization**
- Study lead: **David Vizcaya, George Argyriou**
- Study lead forums tag: **[david_vizcaya](https://forums.ohdsi.org/u/david_vizcaya/), [george_argyriou](https://forums.ohdsi.org/u/george_argyriou/)**
- Study start date: **May 22, 2020**
- Study end date: **March 18, 2021**
- Protocol: **[documents](https://github.com/ohdsi-studies/OT2DSI/tree/master/documents/)**
- Publications: **-**
- Results explorer: **-**


Study Description
==============================

The aim of this study is to identify common occurrence patterns of specific complications in patients with T2D: CKD, DR, DNeu, HF, CVD and CeVD. These patterns will be assessed by population, age at T2D diagnosis and calendar year.
The primary objective in this study is to characterize the occurrence and its ordered sequence of certain chronic conditions in adult patients with T2D by year, population (database) and age category.
Secondary objectives:

-	The average time-to-event since T2D diagnosis for CKD, DR, DNeu, HF, CVD and CeVD
-	The incidence rate of CKD, DR, DNeu, HF, and CVD and CeVD.  

Requirements
============

- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, IBM Netezza, Apache Impala, Amazon RedShift, or Microsoft APS.
- R version 3.6.3
- RTools version 4.0
- Rjava
- Tidyverse R package version 1.3.0
- SqlRender OHDSI R package version 1.6.8
- DatabaseConnector OHDSI R package version 3.0.0
- FeatureExtraction OHDSI R package version 3.1.0



How to run
==========
1. In `R`, use the following code to install the dependencies:

	```r
	install.packages("tidyverse")
	install.packages("jsonlite")
	install.packages("RColorBrewer")
	install.packages("devtools")
	library(devtools)
	install_github("ohdsi/SqlRender")
	install_github("ohdsi/DatabaseConnector")
	install_github("ohdsi/FeatureExtraction")
	```

	If you experience problems on Windows where rJava can't find Java, one solution may be to add `args = "--no-multiarch"` to each `install_github` call, for example:
	
	```r
	install_github("ohdsi/SqlRender", args = "--no-multiarch")
	```
	
	
2. In `R`, use the following code to install the OT2DSI package:

	```r
	install_github("ohdsi-studies/OT2DSI", args = "--no-multiarch")
	```
	
3. Once installed, please execute the script below.

	```r
	library(OT2DSI)

	# The folder where the study result files will be written:
	outputFolder <- "database_name"

	# Details for connecting to the server:
	# See ?DatabaseConnector::createConnectionDetails for help
	connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
									server = "some.server.com/database",
									user = "joe",
									password = "secret",
									port = 5439)
	
	# The name of the database schema where the CDM data can be found:
	cdmDatabaseSchema <- "cdm_synpuf"

	# The name of the database schema and table where the study-specific cohorts will be instantiated:
	targetDatabaseSchema <- "scratch.dbo"
	cohortTable <- "my_study_cohorts"

	# For Oracle: define a schema that can be used to emulate temp tables:
	oracleTempSchema <- NULL
	
	# Connect to the server
	con <- DatabaseConnector::connect(connectionDetails)
        
	  
	# To execute the analysis, run the command below:
	OT2DSI::execute(connection = con,
			cdmDatabaseSchema = cdmDatabaseSchema,
			targetDatabaseSchema = targetDatabaseSchema,
			oracleTempSchema = oracleTempSchema,
			cohortTable = cohortTable,
			outputFolder = outputFolder,
			createCohorts = TRUE,
			generatePathways = TRUE,
			plotResults = TRUE,
			getCharacterization = TRUE,
			getIncidenceRates = TRUE)
	  ```

5. Please send the output.zip file in the output folder to the study coordinator (George Argyriou - george.argyriou@iqvia.com)
