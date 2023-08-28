 # Google analytics Python API

This script allows users to extract data from Google Analytics 4 using the Analytics Data API.

### Prerequisites
- Python 3.x
- Google Analytics 4 property ID
- Service token for the Google Analytics 4 property
- getRefreshToken.py script to obtain the refresh token

### Required Python Libraries
- pandas
- collections
- os.path
- google-auth
- google-auth-oauthlib
- google-auth-httplib2
- google-api-python-client

### Installation
1. Install the required Python libraries
2. Download the getRefreshToken.py script and run it to obtain the refresh token
3. Copy the ga4_data_extraction.py script to your working directory

### Usage
1. Import the necessary libraries
```python
import pandas as pd
from collections import defaultdict
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
```
2. Define the necessary variables

### Define variables
```python
property_id = "GA4 PROPERTY ID"
dimensions = ["DIMENSION 1", "DIMENSION 2"]
metrics = ["METRIC 1", "METRIC 2"]
startDate = "YYYY-MM-DD"
endDate = "YYYY-MM-DD"
ga4token = "SERVICE_TOKEN_FILENAME.json"
```
3. Call the get_data_from_ga4() function
```python
df = get_data_from_ga4(ga4token, property_id, dimensions, metrics, startDate, endDate)
```
4. df will contain a pandas DataFrame with the extracted data.

### Function Reference
**ga_auth(scopes, ga4token)**

This function authenticates the user and builds the Google Analytics service.
- scopes: list of scopes to be authorized.
- ga4token: filename of the service token.

**get_data_from_ga4(ga4token, property_id, dimensions, metrics, startDate, endDate)**

This function extracts data from the Google Analytics 4 property and returns a pandas DataFrame.
- ga4token: filename of the service token.
- property_id: Google Analytics 4 property ID.
- dimensions: list of dimensions to be extracted.
- metrics: list of metrics to be extracted.
- startDate: start date of the data to be extracted.
- endDate: end date of the data to be extracted.

# How to use a JSON data schema to extract data from Google Analytics 4

In order to extract data from Google Analytics 4 and push it easier into Bigquery, you can use a JSON data schema that defines the dimensions and metrics you want to extract. Here is a step-by-step guide on how to use a JSON data schema to extract data from Google Analytics 4 :

### Define the JSON data schema
Create a JSON data schema file that defines the dimensions and metrics you want to extract. Here is an example of a JSON data schema file:

```json

{
    "dimensions":
    [
        {
            "name": "date", 
            "type": "STRING", 
            "mode": "REQUIRED"
        }, 
        {
            "name": "landingPagePlusQueryString", 
            "type": "STRING", 
            "mode": "NULLABLE"
        }, 
        {
            "name": "sessionCampaignName", 
            "type": "STRING", 
            "mode": "NULLABLE"
        }, 
        {
            "name": "sessionMedium", 
            "type": "STRING", 
            "mode": "NULLABLE"
        }, 
        {
            "name": "sessionSource", 
            "type": "STRING", 
            "mode": "NULLABLE"
        }
    ],
    "metrics": 
    [
        {
            "name": "bounceRate", 
            "type": "FLOAT64", 
            "mode": "NULLABLE"
        }, 
        {
            "name": "conversions", 
            "type": "INTEGER", 
            "mode": "NULLABLE"
        }, 
        {
            "name": "sessions", 
            "type": "INTEGER", 
            "mode": "NULLABLE"
        }
    ]
}
```
In this example, we are defining five dimensions (date, landingPagePlusQueryString, sessionCampaignName, sessionMedium, sessionSource) and three metrics (bounceRate, conversions, sessions).

### Load the JSON data schema
Load the JSON data schema from the file using the following code:

```python
import json

file_path = 'table-schema/pull-ga4-config.json'
with open(file_path) as f:
    data = json.loads(f.read())
```
This will load the JSON data schema from the file and store it in the data variable.

### Extract dimensions and metrics from the JSON data schema
Extract the dimensions and metrics from the JSON data schema using the following code:

```python
dimensions = [ele['name'] for ele in data['dimensions']]
metrics = [ele['name'] for ele in data['metrics']]
```
This will extract the dimension and metric names from the JSON data schema and store them in the dimensions and metrics variables.

Use the dimensions and metrics to extract data from Google Analytics 4
You can now use the extracted dimensions and metrics to extract data from Google Analytics 4. Here is an example of how to extract data using the `get_data_from_ga4` function from the previous code snippet:

```python
property_id = 'properties/12345678' # Replace with your GA4 property ID
start_date = '2022-01-01'
end_date = '2022-01-31'
df = get_data_from_ga4(ga4token, property_id, dimensions, metrics, start_date, end_date)
```
This will extract data for the specified dimensions and metrics from the GA4 property with ID ga:12345678 for the month of January 2022, and store the data in a pandas DataFrame called df.

### Notes
- This script is designed to extract data from Google Analytics 4 only.
- The getRefreshToken.py script must be used to obtain the refresh token before using this script.
- The service token must be stored in a JSON file in the same directory as this script.