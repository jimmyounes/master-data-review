from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import json
import os
import re
import httplib2 
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

ga4token = 'key.json'
'''function check whether file exist in the path or not'''
file_name = ga4token
def where_json(file_name):return os.path.exists(file_name)

''' function return the refresh token with a oauth2 token'''

def get_refresh_token(ga4token,client_id,client_secret):
    CLIENT_ID = client_id
    CLIENT_SECRET = client_secret
    SCOPE = 'https://www.googleapis.com/auth/analytics.readonly'
    REDIRECT_URI = 'http:localhost:8080'
  
    flow = OAuth2WebServerFlow(client_id=CLIENT_ID,client_secret=CLIENT_SECRET,scope=SCOPE,redirect_uri=REDIRECT_URI,prompt='select_account')
    if where_json(ga4token)==False:
       storage = Storage(ga4token) 
       credentials = run_flow(flow, storage)
       refresh_token=credentials.refresh_token
       
    elif where_json(ga4token)==True:
       with open(ga4token) as json_file:  
           refresh_token=json.load(json_file)['refresh_token']
  
    return(refresh_token)

client_id = ''
client_secret = ''
print(get_refresh_token(ga4token,client_id,client_secret))