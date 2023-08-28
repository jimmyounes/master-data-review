from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import json 
import os


"""
    Check if a file exists in directory .

    Args:
        parameter : File name  .
      
    Returns:
        bool: True if successful, False otherwise.
"""

                
def where_json(file_name):return os.path.exists(file_name)




"""
    function return the refresh token with a oauth2 token and create one if it doesn't exist .

    Args:
        parameter (1): Token file  .
        parameter (2) :  ID client of  OAuth 2.0 
        parameter (3) : Client secret of the ID client .
      
    Returns:
        Object : return the token associated .
"""

def get_refresh_tokenForGCP(ga4token,client_id,client_secret):
    CLIENT_ID = client_id
    CLIENT_SECRET = client_secret
    SCOPE = 'https://www.googleapis.com/auth/webmasters'
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
