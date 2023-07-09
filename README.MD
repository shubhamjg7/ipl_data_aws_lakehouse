# IPL data analysis

We are working for a cricket board which wants to 
analyse cricket match statistics in their data warehouse.

Cricket match data exists in three sources:
1. DyanmoDB (for ingesting ```matches``` table)
2. MySQL RDS (for ingesting ```deliveries``` table)
3. File in S3 (for ingesting ```player_info``` table)

Tools we will use for creaing the lakehouse:
1. S3 - To store data
2. Glue - As compute
3. Athena - For analysis

## <a name="lake_operations"></a>Lake operations

All three source tables will be ingested in lake layer 
first.

Lake tables:
1. ```deliveries```: Containes ball by ball details of 
   each match.
2. ```matches```: Contains details of the match like 
   venue, teams etc.
3. ```player_info```: Contains details of players and 
   their stats.

### To ingest the lake tables follow below steps:
1. Edit ```ipl_hub_load.ipynb ``` to change variables 
   like crawler role, src and tgt connection details etc 
   (Should've created job parameters). 
2. Run below command to create glue job: 

    ```aws glue create-job --name ipl_lake_load --role <Replace with arn of your glue job role> --command Name=glueetl,ScriptLocation=<S3 location of ipl_lake_load.ipynb> --glue-version 3.0'```
3. Run below command to run the glue job:

   ```aws glue start-job-run --job-name ipl_lake_load```
   
   Alternatively, you can run the job using AWS Step Functions which makes orchestration of multiple glue jobs with interdependency easier.
4. Verify by connecting to database ```orka_lake_warehouse``` in Athena.


## Hub operations

We will perform aggregation, transformation, selection 
and filtering on above lake tables to build new tables in 
hub layer.

We are trying to replicate the match statistics on this [espn page](https://www.espncricinfo.com/series/ipl-2017-1078425/sunrisers-hyderabad-vs-royal-challengers-bangalore-1st-match-1082591/full-scorecard).

Hub tables:
1. ```batting_scorecard```: Contains score card of batsman as on espn.
2. ```bowling_scorecard```: Contains score card of bowlers as on espn.
3. ```match_details```: Contains details of match as on espn.
4. ```match_flow_batsman_stats```: Contains batsman stats from match flow as on espn.
5. ```match_flow_inning_wicket_partnership```: Contains partnership stats as on espn.
6. ```match_flow_inning```: Contains inning stats from match as on espn.
7. ```points_table```: Table of points of each IPL team in a season.

You can follow similar steps we used to run lake job to run our notebook ```ipl_hub_load.ipynb```which loads hub_tables and verify the same by connecting to ```orka_hub_warehouse``` in Athena.

#### Now we can use above hub tables to further create mart tables more complex business requirements as needed. 


Note: The input files we used were ```IPL_Deliveries.sqlite```, ```matches.json```, and  ```Players_info.csv```. We are using ```Players_info.csv``` directly for lake ingestion. If you want to ingest  ```IPL_Deliveries.sqlite``` to MySQL RDS you can use ```mysql_rds_cf_template.yaml``` cloud formation template to create the MySQL database and ```deliveries_sqlite_to_rds.ipynb``` to load ```deliveries``` table in that database. Similarly, if you want to ingest ```matches.json``` in a dynamodb table, you can use ```dynamodb_nosql_cf_template.yaml``` to create the dynamodb table and  ```matches_json_to_dynamodb.py``` script to load the table using glue job. The created tables in MySQL and DynamoDB can then be used for [Lake operations](lake_operations) done above.

