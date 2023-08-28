

from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from typing import Dict
import pandas as pd
import json 
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


"""
   The function returns a Google Cloud Platform (GCP) service after authenticating using a token file

    Args:
        parameter (1): Token file   .
       
      
    Returns:
        Object : return the GCP service  .
"""


def AuthentificateAndGetServiceGCP(key_file):
    # Initialize a Google_Client
    with open(key_file, 'r') as f:
        key_data = json.load(f)
    # Specify the scopes
    scopes = ['https://www.googleapis.com/auth/webmasters']
    # Create credentials from the loaded key data
    creds = Credentials.from_authorized_user_info(key_data, scopes)
      # Build the service client
    service = build('webmasters', 'v3', credentials=creds)
    # Create a service object for interacting with the Search Console API
    
    
    return service


"""
   The function returns a list of domain associated to the GCP service from a token file .

    Args:
        parameter (1): Token file   .
       
      
    Returns:
        list of DOMAINS  .
"""

def GetAllDomainsFromService(key_file):
    return AuthentificateAndGetServiceGCP(key_file).sites().list().execute()


"""
    The function returns a list of data analytics results associated with a specific domain. This query retrieves a maximum of 25,000 results.

    Args:
        parameter (1): Google Cloud Platform (GCP) service.
        parameter (2): The URL of the domain.
        payload: A dictionary containing query constraints.

    Returns:
        dict: Data analytics results from the appropriate query."

"""
def query(client: Resource,DOMAIN ,  payload: Dict[str, str]) -> Dict[str, any]:
    response = client.searchanalytics().query(siteUrl=DOMAIN, body=payload).execute()
    return response

""" 
    The function returns a DataFrame containing data analytics results obtained by creating a query associated with a group of metrics.

    Args:

        parameter (1): Start date of the query.
        parameter (2): The end date of the query.
        parameter (3): Dimensions associated with the query.
        parameter (4): Maximum rows supported by the query. (Here 25k)
        parameter (5): The requested domain.

    Returns:

        DataFrame: A DataFrame of data analytics results obtained with the specified dimensions. "

"""

def GetDataOfQuery( START_DATE,end_date, DIMENSIONS,DOMAIN,service, MAX_ROWS=25_000):
    i=0
    response_rows = []
    while True:
        payload = {
        "startDate": START_DATE,
        "endDate": end_date,
        "dimensions": DIMENSIONS,
        "rowLimit": MAX_ROWS,
        "searchType": "Web",
        "startRow": i * MAX_ROWS,
        }

    # make request to API
        response = query(service,DOMAIN, payload)

    # if there are rows in the response append to the main list
        if response.get("rows"):
            response_rows.extend(response["rows"])
            i += 1
        else:
            break

    print(f"Collected {len(response_rows):,} rows")


    df = pd.DataFrame(response_rows)
    df.head()
    keys_df = df['keys'].apply(pd.Series)
    keys_df.columns =DIMENSIONS
    # Concatenate the new columns with the existing DataFrame
    final_df = pd.concat([keys_df, df.drop(columns='keys')], axis=1)
   
    
    return final_df





