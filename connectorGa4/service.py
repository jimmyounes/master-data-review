# Important, ga4 connection needs a oauth 2 service token
# This service token need a refresh key => using the getRefreshToken.py script

# Import Modules
import pandas as pd
from collections import defaultdict
# Import Modules
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import json 

def ga_auth(scopes, ga4token):

    creds = None
    # The file DigitalUncutOauth2token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if '.json' not in ga4token :
        ga4token = json.loads(ga4token)
        creds = Credentials.from_authorized_user_info(ga4token, scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                print("Error on ga4 with env variable")
    else :
        if os.path.exists(ga4token):
            creds = Credentials.from_authorized_user_file(ga4token, scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    ga4token, scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(ga4token, 'w') as token:
                token.write(creds.to_json())

    service = build('analyticsdata', 'v1beta', credentials=creds,cache_discovery=False)
    return service


def get_data_from_ga4(ga4token,property_id, dimensions, metrics, startDate, endDate): 
    # Define scopes
    scopes = ['https://www.googleapis.com/auth/analytics.readonly']
    # Authenticate & Build Service
    analytics = ga_auth(scopes, ga4token)
    # Build Request Body
    request = {
    "requests": [
        {
        "dateRanges": [
            {
            "startDate": startDate,
            "endDate": endDate
            }
        ],
        "dimensions": [{'name': name} for name in dimensions],
        "metrics": [{'name': name} for name in metrics],
        "limit": 100000
        }
    ]
    }

    # Make Request
    response = analytics.properties().batchRunReports(property=property_id, body=request).execute()
    # Parse Request
    report_data = defaultdict(list)

    for report in response.get('reports', []):
        rows = report.get('rows', [])
        for row in rows:
            for i, key in enumerate(dimensions):
                report_data[key].append(row.get('dimensionValues', [])[i]['value'])  # Get dimensions
            for i, key in enumerate(metrics):
                report_data[key].append(row.get('metricValues', [])[i]['value'])  # Get metrics

    df = pd.DataFrame(report_data)
    return df

def get_data_from_ga4_SchemaFile(ga4token, tableSchemaFileData, property_id,startDate, endDate):  
    data = tableSchemaFileData
    dimensions = [ele['name'] for ele in data['dimensions']]
    metrics = [ele['name'] for ele in data['metrics']]

    df = get_data_from_ga4(ga4token,property_id, dimensions, metrics, startDate, endDate)

    # Change type of data to correspond to the schema file
    dimAndMetrics = [(ele['name'],ele['type']) for ele in data['dimensions']]
    dimAndMetrics.extend([(ele['name'],ele['type']) for ele in data['metrics']])
    for namex, type in dimAndMetrics : 
        if type == 'INTEGER' :
            type = 'int'
        df = df.astype({namex:type.lower()})

    return df

