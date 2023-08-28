from connectorGa3.serviceUA import *
from connectorGoogleSheet.service import *
from connectorBigquery.service import *
from connectorGa4.service import *
from connectorGoogleAds.service import *

import pandas as pd 
import requests
import json
import httplib2

# Google librairies
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools

from datetime import datetime


########## Logs on slack ###########
def logs_to_slack(slack_app, logger):
    try :
        f  = open('logging_.log', "r")
        d=f.read()
        dstart = d.split("\n")
        dstart.remove('')
        dsplit = [i.split('|') for i in dstart]
        for line in dsplit :
            if ' Start main.py' in line :
                last_list = []
            last_list.append('|'.join(line))
        d = '\n'.join(last_list)
        d = d.replace('WARNING',':warning: *WARNING*')
        d = '*Hi team*, here you can find the latest logs of the master-data project. Have a nice day! :smile: \n'+d
        headers = {
            'Content-type': 'application/json',
        }
        json_data = {'text' : d}
        logger.info('Succeed on log to slack')
        return requests.post(slack_app, headers=headers, json=json_data)      
    except Exception as exception_code :
        logger.warning('Failed on log to slack')
        logger.warning('Exception '+str(exception_code)) 

########## Clients choices #########
def get_data_from_client_table(client_table_id, logger, GOOGLE_APPLICATION_CREDENTIALS, project_id):
    try :    
        #to pull data from the base client
        query = "SELECT * FROM `"+client_table_id+"`"
        dataframe_client = pull_data_from_bigquery_table(query,GOOGLE_APPLICATION_CREDENTIALS,project_id)
        #pull the last changes
        dataframe_client = dataframe_client.groupby(['clientId','source']).last().reset_index()
        #logger.info('Succeed on update client table')        
        return dataframe_client
    except Exception as exception_code :
        logger.warning('Failed on update client table')
        logger.warning('Exception '+str(exception_code))
                   
def config_consultants_choices(REQUIRED_CONFIG_KEYS, spreadsheet_id, sheet_name, range):
    dataframe_client = fetch_sheet(REQUIRED_CONFIG_KEYS, spreadsheet_id, sheet_name,range)

    dataframe_client.columns = ["demandeDate", "clientName", "consultantName", "consultantEmail","historicalDate","GA3","GA3clientId","GA4","GA4clientId","clientType"]

    ga3df = dataframe_client.loc[dataframe_client['GA3']=='Yes'][['clientName', 'GA3clientId', 'clientType', 'historicalDate']]
    ga3df.columns = ['clientName', 'clientId', 'clientType', 'historicalDate']
    ga3df['source'] = ['GA3' for ele in ga3df['clientName']]

    ga4df = dataframe_client.loc[dataframe_client['GA4']=='Yes'][['clientName', 'GA4clientId', 'clientType', 'historicalDate']]
    ga4df.columns = ['clientName', 'clientId', 'clientType', 'historicalDate']
    ga4df['source'] = ['GA4' for ele in ga4df['clientName']]

    dataframe_client_global = pd.concat([ga3df,ga4df])

    dataframe_client_global = dataframe_client_global.groupby(['source', 'clientName', 'clientId', 'clientType', 'historicalDate']).first()
    dataframe_client_global = dataframe_client_global.reset_index()
    dataframe_client_global['state'] = ['New' for ele in dataframe_client_global['clientName']]
    dataframe_client_global['lastUpdate'] = ['' for ele in dataframe_client_global['clientName']]
    dataframe_client_global['status'] = ['' for ele in dataframe_client_global['clientName']]
    dataframe_client_global['_extracted_at'] = [datetime.utcnow() for ele in dataframe_client_global['clientName']]
    return dataframe_client_global

def get_dataframe_client_from_bq_sheet(REQUIRED_CONFIG_KEYS, spreadsheet_id, sheet_name, range, query, GOOGLE_APPLICATION_CREDENTIALS, project_id):
    #create_a_new_biqquery_table(tableSchemaFile, client_table_id, GOOGLE_APPLICATION_CREDENTIALS, project_id)
    dataframe_client_fromSheet = config_consultants_choices(REQUIRED_CONFIG_KEYS, spreadsheet_id, sheet_name, range)

    try :
        dataframe_client_from_bq = pull_data_from_bigquery_table(query, GOOGLE_APPLICATION_CREDENTIALS, project_id)
        list_client_bq = dataframe_client_from_bq['clientId']        
        df = pd.concat([dataframe_client_from_bq,dataframe_client_fromSheet[~dataframe_client_fromSheet['clientId'].isin(list_client_bq)]])
    except :
        df = dataframe_client_fromSheet 
    return df
########## GA4 ########
def get_data_from_ga4_for_client(clientId, table_id, ga4token, tableSchemaFileData, startDate,  endDate, logger, line, dataframe_client):    
    property_id = 'properties/'+ clientId
    try :
        dataframe = get_data_from_ga4_SchemaFile(ga4token, tableSchemaFileData, property_id, startDate, endDate)
        dataframe['clientId'] = [clientId for ele in dataframe[dataframe.columns[0]]]
        dataframe['clientName'] = [line.clientName for ele in dataframe[dataframe.columns[0]]]
        dataframe['_extracted_at'] = [datetime.utcnow() for ele in dataframe[dataframe.columns[0]]]        
        logger.info('Pull dataframe from ga4 for client: '+property_id)
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Succeed on collect'
        return dataframe
    except Exception as exception_code:
        logger.warning('Failed on pull data from ga4 for client: '+property_id)
        logger.warning('Exception '+str(exception_code))
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Failed on collect'
######### GA3 ##########
def get_data_from_ga3_for_client(ga3token,tableSchemaFileData, clientId,startDate, endDate, logger, line, dataframe_client):
    try :
        dataframe = get_data_from_ga3_SchemaFile(ga3token,tableSchemaFileData, clientId,startDate, endDate)
        dataframe['clientId'] = [clientId for ele in dataframe[dataframe.columns[0]]]
        dataframe['clientName'] = [line.clientName for ele in dataframe[dataframe.columns[0]]]
        dataframe['_extracted_at'] = [datetime.utcnow() for ele in dataframe[dataframe.columns[0]]]
        dataframe.columns = [i.replace('ga:','') for i in dataframe.columns]
        logger.info('Pull dataframe from ga3 for client: '+clientId)
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Succeed on collect'
        return dataframe
    except Exception as exception_code:
        logger.warning('Failed on pull data from ga3 for client: '+clientId)
        logger.warning('Exception '+str(exception_code))
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Failed on collect'

def account_summaries_json_to_dataframe(ga3token, logger):
    try :
        analytics = New_ga_auth(ga3token,'v3')
        json_string = analytics.management().accountSummaries().list().execute()
        # Extraire la liste des comptes
        accounts = json_string['items']
        # Créer une liste de dictionnaires pour chaque compte
        account_list = []
        for account in accounts:
            # Extraire les propriétés du compte
            account_id = account['id']
            account_name = account['name']

            # Extraire les propriétés de chaque propriété Web
            for web_property in account['webProperties']:
                web_property_id = web_property['id']
                web_property_name = web_property['name']
                internalWebPropertyId = web_property['internalWebPropertyId']
                try :
                    website_url = web_property['websiteUrl']
                    web_property_level = web_property['level']
                except : 
                    website_url = 'Nan'
                    web_property_level = 'Nan'
                # Extraire les propriétés de chaque profil
                for profile in web_property['profiles']:
                    profile_id = profile['id']
                    profile_name = profile['name']
                    profile_type = profile['type']

                    # Ajouter une ligne au tableau
                    account_list.append({
                        'clientId': account_id,
                        'clientName': account_name,
                        'web_property_id': web_property_id,
                        'web_property_name': web_property_name,
                        'website_url': website_url,
                        'internalWebPropertyId': internalWebPropertyId,
                        'web_property_level': web_property_level,
                        'profile_id': profile_id,
                        'profile_name': profile_name,
                        'profile_type': profile_type
                    })

        # Créer un DataFrame à partir de la liste de dictionnaires
        df = pd.DataFrame(account_list)
        return df 
    except Exception as exception_code:
        logger.warning('Failed on on GA3 account_summaries_json_to_dataframe for client: '+clientId)
        logger.warning('Exception '+str(exception_code))


def goals_json_to_dataframe(data, logger):
    try :
        data = data['items']
        rows = []
        for item in data:
            row = {
                'id': item['id'],
                'kind': item['kind'],
                'selfLink': item['selfLink'],
                'clientId': item['accountId'],
                'webPropertyId': item['webPropertyId'],
                'internalWebPropertyId': item['internalWebPropertyId'],
                'profileId': item['profileId'],
                'name': item['name'],
                'value': item['value'],
                'active': item['active'],
                'type': item['type'],
                'created': item['created'],
                'updated': item['updated'],
                'parentLinkType': item['parentLink']['type'],
                'parentLinkHref': item['parentLink']['href']
            }
            if 'urlDestinationDetails' in item:
                row['url'] = item['urlDestinationDetails']['url']
                row['caseSensitive'] = item['urlDestinationDetails']['caseSensitive']
                row['matchType'] = item['urlDestinationDetails']['matchType']
                row['firstStepRequired'] = item['urlDestinationDetails']['firstStepRequired']
                row['numSteps'] = len(item['urlDestinationDetails']['steps'])
                if len(item['urlDestinationDetails']['steps']) > 0:
                    for i, step in enumerate(item['urlDestinationDetails']['steps']):
                        if 'path' in step :
                            row[f'step{i+1}Path'] = step['path']
                        else :
                            row[f'step{i+1}Path'] = 'None'
                        row[f'step{i+1}Name'] = step['name']
                        row[f'step{i+1}Number'] = step['number']
            if 'eventDetails' in item:
                row['eventUseValue'] = item['eventDetails']['useEventValue']
                row['eventCategory'] = None
                row['eventAction'] = None
                row['eventLabel'] = None
                for condition in item['eventDetails']['eventConditions']:
                    if condition['type'] == 'CATEGORY':
                        row['eventCategory'] = condition['expression']
                    elif condition['type'] == 'ACTION':
                        row['eventAction'] = condition['expression']
                    elif condition['type'] == 'LABEL':
                        row['eventLabel'] = condition['expression']
            rows.append(row)
        df = pd.DataFrame(rows)
        return df
    except Exception as exception_code:
        logger.warning('Failed on on GA3 goals_json_to_dataframe for client: '+clientId)
        logger.warning('Exception '+str(exception_code))

   

def get_goals_dataframe(ga3token, df_account_summaries, logger) :
    try :
        df_all = pd.DataFrame()
        for client in df_account_summaries.itertuples() :
            analytics = New_ga_auth(ga3token,'v3')
            goals = analytics.management().goals().list(
                accountId=client.clientId,
                webPropertyId=client.web_property_id,
                profileId=client.profile_id).execute()
            df_client = goals_json_to_dataframe(goals, logger)
            df_client['clientName'] = [client.clientName for ele in range(len(df_client))]
            #df_client['_extracted_at'] = [datetime.utcnow() for ele in df_client[df_client.columns[0]]]

            for namex in df_client.columns : 
                type = 'STRING'
                df_client = df_client.astype({namex:type.lower()})
            if df_all.empty :
                df_all = df_client
            df_all = pd.concat([df_all,df_client])
        for namex in df_all.columns : 
            type = 'STRING'
            df_all = df_all.astype({namex:type.lower()})

    except Exception as exception_code:
        logger.warning('Failed on on GA3 get_goals_dataframe for client: '+clientId)
        logger.warning('Exception '+str(exception_code))

   
    return df_all

def load_account_summaries_and_goals_to_bq(dataframe, tableSchemaFileData, project_id, database_id, table_name, GOOGLE_APPLICATION_CREDENTIALS, logger) :
    try :
        list_for_json = []
        for key in dataframe.keys():
            list_for_json.append({
                "name": key,
                "type": "STRING",
                "mode": "NULLABLE"})
        dataframe['_extracted_at'] = [datetime.utcnow() for ele in dataframe[dataframe.columns[0]]]

        tableSchemaFileData = {"dimensions" : list_for_json}
        table_id = '{}.{}.{}'.format(project_id, database_id, table_name)
        try :
            create_a_new_biqquery_table(tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id)
        except :
            1
        load_truncate_table_from_dataframe(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id)        
    except Exception as exception_code:
        logger.warning('Failed on GA3 load_account_summaries_and_goals_to_bq')
        logger.warning('Exception '+str(exception_code))

def get_data_from_ga3_goal_SchemaFile(ga3token, df_client_goals_unique, tableSchemaFile, VIEW_ID, startDate, endDate,logger, line) :
    try :
        listNumberGoals = [re.findall('goals\/(\d+)',ele)[0] for ele in df_client_goals_unique['selfLink']]
        listGoalsMetrics = ['ga:goalXXStarts', 'ga:goalXXCompletions', 'ga:goalXXValue', 'ga:goalXXConversionRate']
        df_all = pd.DataFrame()
        with open(tableSchemaFile) as f:
            tableSchemaFileData = json.loads(f.read())    
        tableSchemaFileData['metrics'].extend([{'name': metricName.replace('XX',''), 
                    "type": "FLOAT64", 
                    "mode": "NULLABLE"} for metricName in listGoalsMetrics])
        tableSchemaFileData['dimensions'].extend([{'name': 'goal', 
                    "type": "STRING", 
                    "mode": "NULLABLE"}])                  
        
        for i in listNumberGoals : 
            with open(tableSchemaFile) as f:
                tableSchemaFileData2 = json.loads(f.read())    
            
            tableSchemaFileData2['metrics'].extend([{'name': metricName.replace('XX',str(i)), 
                        "type": "FLOAT64", 
                        "mode": "NULLABLE"} for metricName in listGoalsMetrics])
            try :
                df_prod = get_data_from_ga3_SchemaFile(ga3token,tableSchemaFileData2, VIEW_ID, startDate, endDate)
                print(df_prod)
                df_prod['goal'] = [str(i) for ele in range(len(df_prod))]
                df_prod.columns = [ele.replace(str(i),'') for ele in df_prod.columns]
                df_prod['clientId'] = [line.clientId for ele in df_prod[df_prod.columns[0]]]
                df_prod['clientName'] = [line.clientName for ele in df_prod[df_prod.columns[0]]]  
                df_prod['_extracted_at'] = [datetime.utcnow() for ele in df_prod[df_prod.columns[0]]]
                df_prod.columns = [i.replace('ga:','') for i in df_prod.columns]              
                if df_all.empty :
                    df_all = df_prod
                else :
                    df_all = pd.concat([df_all,df_prod])
  
            except Exception as exception_code :
                print('Exception '+str(exception_code))
                df_prod = pd.DataFrame()   
        return df_all,tableSchemaFileData
    
    except Exception as exception_code:
        logger.warning('Failed on pull data from google analytics UA Goals for client: '+line.clientId)


        
########## Google ads ###########
def get_data_from_gads_for_client(gadstoken,tableSchemaFileData, clientId,startDate, endDate, logger, line, dataframe_client):
    try :
        dataframe = get_data_from_gads_schemaFile_format(tableSchemaFileData,clientId, startDate, endDate,gadstoken)
        dataframe['clientId'] = [clientId for ele in dataframe[dataframe.columns[0]]]
        dataframe['clientName'] = [line.clientName for ele in dataframe[dataframe.columns[0]]]
        dataframe['_extracted_at'] = [datetime.utcnow() for ele in dataframe[dataframe.columns[0]]]
        dataframe.columns = [i.replace('ga:','').replace('.','_') for i in dataframe.columns]
        logger.info('Pull dataframe from google ads for client: '+clientId)
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Succeed on collect'
        return dataframe
    except Exception as exception_code:
        logger.warning('Failed on pull data from google ads for client: '+clientId)
        logger.warning('Exception '+str(exception_code))
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Failed on collect'
        
########### BIG Query ############
def create_bigquery_table_with_logs(tableSchemaFileData,database_id,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id, logger) :
    #to create a new table if she doesn't exist
    try:
        create_a_new_biqquery_table(tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id)
        #logger.info("Created table {}.{}.{}".format(project_id, database_id, table_id)+ ' on bigquery')
    except:
        1
        #logger.info("Table {}.{}.{}".format(project_id, database_id, table_id)+' already exists on bigquery')
        
def push_data_into_table(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id,logger,line,database_id,dataframe_client):
    try :
        load_table_from_dataframe(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id)
        logger.info("Data loaded to {}.{}.{}".format(project_id, database_id, table_id)+ ' on bigquery')
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Succeed on push into Bigquery'
    except Exception as exception_code:
        logger.warning("Failed on load data to {}.{}.{}".format(project_id, database_id, table_id)+ ' on bigquery')
        logger.warning('Exception '+str(exception_code))
        mask = (dataframe_client['clientId'] == line.clientId) & (dataframe_client['source'] == line.source)
        dataframe_client.loc[mask, 'status'] = 'Failed on push into Bigquery'
    
