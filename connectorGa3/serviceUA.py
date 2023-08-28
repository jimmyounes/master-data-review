import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import argparse
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
import json
import pandas as pd
def New_ga_auth(ga3token,version):
    if version=='v3':
        task='analytics'
    else : 
        task='analyticsreporting'

    scopes = ['https://www.googleapis.com/auth/analytics.readonly']
    creds = None
    # The file DigitalUncutOauth2token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if '.json' not in ga3token :
        ga3token = json.loads(ga3token)
        creds = Credentials.from_authorized_user_info(ga3token, scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                print("Error on ga3 with env variable")
    else :
        if os.path.exists(ga3token):
            creds = Credentials.from_authorized_user_file(ga3token, scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    ga3token, scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(ga3token, 'w') as token:
                token.write(creds.to_json())

    service = build(task, version, credentials=creds, cache_discovery=False)
    return service


def res_to_df(res):
    report = res['reports'][0]
    dimensions = report['columnHeader']['dimensions']
    metrics = [m['name'] for m in report['columnHeader']['metricHeader']['metricHeaderEntries']]
    headers = [*dimensions, *metrics]
    
    data_rows = report['data']['rows']
    data = []
    for row in data_rows:
        data.append([*row['dimensions'], *row['metrics'][0]['values']])
    
    return pd.DataFrame(data=data, columns=headers)

def get_data_from_ga3(VIEW_ID,ga3token,
    #default date range 
    start_date='3daysAgo', end_date='yesterday', 
    metrics=[], dimensions=[]
    ):
    # Build the service object. en version V4 de Google Analytics API
    analyticsV4 = New_ga_auth(ga3token,'v4')
    response = analyticsV4.reports().batchGet(
            body={
                'reportRequests': [{
                    'viewId': VIEW_ID,
                    'pageSize': 100000,
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': [{'expression': m} for m in metrics],
                    'dimensions': [{'name': d} for d in dimensions],
                }]
            }).execute()
    response_all = response.copy()
    while 'nextPageToken' in response['reports'][0].keys() :
        response = analyticsV4.reports().batchGet(
                body={
                    'reportRequests': [{
                        'viewId': VIEW_ID,
                        'pageSize': 100000,  #pour dépasser la limite de 1000
                        'pageToken': response['reports'][0]['nextPageToken'],
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'metrics': [{'expression': m} for m in metrics],
                        'dimensions': [{'name': d} for d in dimensions],
                    }]
            }).execute() 
        response_all['reports'][0]['data']['rows'].extend(response['reports'][0]['data']['rows'])
    return res_to_df(response_all)

def get_data_from_ga3_SchemaFile(ga3token,tableSchemaFileData, VIEW_ID, startDate, endDate):  
    data = tableSchemaFileData
    dimensions = [ele['name'] for ele in data['dimensions']]
    metrics = [ele['name'] for ele in data['metrics']]
    # Mettre une boucle si d'autres données sur ce client prend seulement le dernier jour sinon prend les x derniers jours
    # if new client depend on what they want else last 2 days
    # new client => information gave by google forme => big query table
    # Update the client table with the date on run 

    df = get_data_from_ga3(
                VIEW_ID, ga3token,
                startDate, endDate,
                metrics, dimensions
                )

    # Change type of data to correspond to the schema file
    dimAndMetrics = [(ele['name'],ele['type']) for ele in data['dimensions']]
    dimAndMetrics.extend([(ele['name'],ele['type']) for ele in data['metrics']])
    for namex, type in dimAndMetrics : 
        if type == 'INTEGER' :
            type = 'int'
        df = df.astype({namex:type.lower()})
    print(df)

    return df