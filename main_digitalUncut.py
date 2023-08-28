import os
from dotenv import load_dotenv
#from connectorBigquery.service import *
#from service import *
import logging
import sys
from datetime import datetime, timedelta

from connectorBigquery.service import *
from connectorGa4.service import *
from connectorGa3.serviceUA import *
from connectorGoogleAds.service import *

from service import *
import requests

        
# Define the env variables
load_dotenv()

# set the variable in gcp environnement before use it
gatoken = os.environ['ga4token']
gadstoken = os.environ['gadstoken']
REQUIRED_CONFIG_KEYS = os.environ['REQUIRED_CONFIG_KEYS']

try :
    GOOGLE_APPLICATION_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
except :
    GOOGLE_APPLICATION_CREDENTIALS = os.environ["google_secret_bigquery"]
#A supp : 

slack_app = os.environ['slack_app']
project_id = os.environ['PROJECT_ID']
database_id = os.environ['DATABASE_ID']

# table client to pull needs / refresh by client & source
client_table = os.environ['CLIENT_TABLE']
GA4Table_name = os.environ['GA4_TABLE_NAME']
GA4_ECOMMERCE_TABLE_NAME = os.environ['GA4_ECOMMERCE_TABLE_NAME']
GA4_EVENT_TABLE_NAME = os.environ['GA4_EVENT_TABLE_NAME']
GA3Table_name = os.environ['GA3_TABLE_NAME']
GadsTable_name = os.environ['GADS_TABLE_NAME']
accountTable_name = os.environ['accountTable_name']
goalsTable_name = os.environ['goalsTable_name']
GA3GoalTable_name = os.environ['GA3GoalTable_name']

#client table
clientTableSchemaFile = 'tableSchema/client_digitalUncut.json'
GA4TableSchemaFile = 'tableSchema/GA4/ga4_lead_generation.json'
GA4ecommerceTableSchemaFile = 'tableSchema/GA4/ga4_ecommerce.json'
GA4eventTableSchemaFile = 'tableSchema/GA4/ga4_event.json'
GA3GoalsTableSchemaFile = "tableSchema/GA3/pull-ga3-goals.json"
accountTableSchemaFile = "tableSchema/GA3/GoogleAnalytics_account_summaries.json"
goalsTableSchemaFile = "tableSchema/GA3/GoogleAnalytics_client_goals.json"

GA3TableSchemaFile = 'tableSchema/GA3/pull-ga3-sessions-config.json'
GadsTableSchemaFile = 'tableSchema/pull-gads-config-kw.json'
client_table_id = '{}.{}.{}'.format(project_id, database_id, client_table)
spreadsheet_id = '1BarJYoadq5HxNY7T6eD_ygsrgBU4b7B3YRgiJEA1CaU'
sheet_name = 'Responses'

#logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)
file_handler = logging.FileHandler('logging_.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

#start logging
logger.info('Start main.py')

#get the client_table to obtain a dataframe
query = "SELECT * FROM `"+client_table_id+"`"
range = 'A:J'
dataframe_client = get_dataframe_client_from_bq_sheet(REQUIRED_CONFIG_KEYS, spreadsheet_id, sheet_name, range, query, GOOGLE_APPLICATION_CREDENTIALS, project_id)


#Account summaries
df_account_summaries = account_summaries_json_to_dataframe(gatoken,logger)
dataframe = df_account_summaries
load_account_summaries_and_goals_to_bq(dataframe, accountTableSchemaFile, project_id, database_id, accountTable_name, GOOGLE_APPLICATION_CREDENTIALS, logger)

#Account goals
df_client_goals = get_goals_dataframe(gatoken, df_account_summaries, logger)
dataframe = df_client_goals
load_account_summaries_and_goals_to_bq(dataframe, goalsTableSchemaFile, project_id, database_id, goalsTable_name, GOOGLE_APPLICATION_CREDENTIALS, logger)
  

#loop to take each line of the client table
for line in dataframe_client.itertuples() :
    df_client_goals_unique = df_client_goals[df_client_goals['profileId']==line.clientId]
    if line.state == 'New' :
        startDate=str((datetime.now()-datetime.strptime(line.historicalDate,'%d/%m/%Y')).days)+'daysAgo'
        endDate='yesterday'
    else :
        startDate='4daysAgo'
        endDate='yesterday'
        
    source = line.source
    clientId = line.clientId
    logger.info(':smile: Start collecte data from : ' +source+ ' client *'+ line.clientName + '* state '+ line.state)
    
    if source == 'GA4' :
        if line.clientType == "Ecommerce":  
            listTableSchema = [GA4ecommerceTableSchemaFile,GA4TableSchemaFile, GA4eventTableSchemaFile]
            listTableName = [GA4_ECOMMERCE_TABLE_NAME, GA4Table_name, GA4_EVENT_TABLE_NAME]         
            
        else :
            listTableSchema = [GA4TableSchemaFile, GA4eventTableSchemaFile]
            listTableName = [GA4Table_name, GA4_EVENT_TABLE_NAME]         


        for tableSchema, tableName in zip(listTableSchema, listTableName):
                tableSchemaFile = tableSchema
                with open(tableSchemaFile) as f:
                    tableSchemaFileData = json.loads(f.read())
                table_id = '{}.{}.{}'.format(project_id, database_id, tableName)   
                #fonction to get data from ga4     
                dataframe = get_data_from_ga4_for_client(clientId, table_id, gatoken, tableSchemaFileData, startDate,  endDate, logger, line, dataframe_client)           
                #fonction to create table if doesn't exist
                create_bigquery_table_with_logs(tableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger)
                push_data_into_table(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id,logger,line,database_id,dataframe_client) 
    elif source == 'GA3' :
        if df_client_goals_unique.empty :
            logger.info('0 goals for this client on GA3')

        else :
            #ajouter collecte goals
            tableSchemaFile = GA3GoalsTableSchemaFile
            print(df_client_goals_unique)

            #Add goals to tableSchemaFileData
            table_id = '{}.{}.{}'.format(project_id, database_id, GA3GoalTable_name)
            #fonction to get data from ga3    
            result = get_data_from_ga3_goal_SchemaFile(gatoken, df_client_goals_unique, tableSchemaFile, clientId, startDate, endDate, logger, line)


            dataframe = result[0]
            tableSchemaFileData = result[1]
            print(dataframe.columns)
            print(result)
            
            if dataframe.empty :
                1
            else :
                #fonction to create table if doesn't exist
                create_bigquery_table_with_logs(tableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger)
                push_data_into_table(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id,logger,line,database_id,dataframe_client)

        
        tableSchemaFile = GA3TableSchemaFile
        with open(tableSchemaFile) as f:
            tableSchemaFileData = json.loads(f.read())
        table_id = '{}.{}.{}'.format(project_id, database_id, GA3Table_name)
        #fonction to get data from ga3    
        dataframe = get_data_from_ga3_for_client(gatoken,tableSchemaFileData, clientId,startDate, endDate, logger, line, dataframe_client)
        #fonction to create table if doesn't exist
        create_bigquery_table_with_logs(tableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger)
        push_data_into_table(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id,logger,line,database_id,dataframe_client)
    elif source == 'GAds' :
        tableSchemaFile = GadsTableSchemaFile
        with open(tableSchemaFile) as f:
            tableSchemaFileData = json.loads(f.read())        
        table_id = '{}.{}.{}'.format(project_id, database_id, GadsTable_name)
        #fonction to get data from google ads
        dataframe = get_data_from_gads_for_client(gadstoken,tableSchemaFileData, clientId,startDate, endDate, logger, line, dataframe_client)
        #fonction to create table if doesn't exist
        create_bigquery_table_with_logs(tableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger)
        push_data_into_table(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id,logger,line,database_id,dataframe_client)

    mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
    dataframe_client.loc[mask, 'lastUpdate'] = str(datetime.now())[:16]

    mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source) & (dataframe_client['status'] == 'Succeed on push into Bigquery') & (dataframe_client['state'] == 'New')
    dataframe_client.loc[mask, 'state'] = 'Old'
    print(dataframe_client)
try :    
    #update client table    
    table_id = '{}.{}.{}'.format(project_id, database_id, client_table)
    with open(clientTableSchemaFile) as f:
        clientTableSchemaFileData = json.loads(f.read())
    create_bigquery_table_with_logs(clientTableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger)
    load_truncate_table_from_dataframe(dataframe_client,clientTableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id)
    logger.info('Succeed on update client table')
except Exception as exception_code :
    logger.warning('Failed on update client table')
    logger.warning('Exception '+str(exception_code))
        
logger.info('Finished')

logs_to_slack(slack_app, logger)