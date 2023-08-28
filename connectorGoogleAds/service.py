from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import pandas as pd
from datetime import datetime, timedelta

# Import the method 
from google.protobuf import json_format
import json
import ast
import sys

def get_schema_file_to_query(tableSchemaFileData, startDate, endDate):
    data = tableSchemaFileData
    query = " SELECT "
    query = query+", ".join([ele['name'] for key in data.keys() for ele in data[key]]) + " FROM keyword_view"+" WHERE segments.date BETWEEN '"+startDate+"' AND '"+endDate+"'"
    return query
    

def getGoogleAdsData(tableSchemaFileData,clientId, startDate, endDate, gadstoken) : 
    data = tableSchemaFileData
    credentials = ast.literal_eval(gadstoken)
    googleads_client = GoogleAdsClient.load_from_dict(credentials)
    client = googleads_client
    ga_service = client.get_service("GoogleAdsService", version="v13")
    query = get_schema_file_to_query(tableSchemaFileData, startDate, endDate)
    df = pd.DataFrame()
    report_request = client.get_type("SearchGoogleAdsStreamRequest")
    report_request.customer_id = clientId
    report_request.query = query
    response = ga_service.search_stream(report_request)

    list_dim_metrics_res = []

    list_dim_metrics_name = [ele['name'] for key in data.keys() for ele in data[key]]
    try:
        # Converting each of the GoogleAdsRow to json
        for row in response : 
            json_str = json_format.MessageToJson(row._pb)
            d = json.loads(json_str)        


    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
        
    return d

def get_data_from_gads_schemaFile_format(tableSchemaFileData,clientId, startDate, endDate,gadstoken) :
    data = tableSchemaFileData
    date = datetime.now()+timedelta(days=-int(startDate.replace('daysAgo','')))
    startDate = date.strftime("%Y-%m-%d")
    date = datetime.now()+timedelta(days=-1)
    endDate = date.strftime("%Y-%m-%d")    
    data_resp = getGoogleAdsData(tableSchemaFileData,clientId, startDate, endDate,gadstoken)

    list_dim_metrics_name = [ele['name'] for key in data.keys() for ele in data[key]]

    df_res = pd.DataFrame()

    for dim in list_dim_metrics_name :
        ## Warning : Name of metrics and dimensions in response dataframe differed from the pull requests
        ### Put a rule : if '_' in the name of dim, replace '_'+ first letter after by a maj of this letter
        newDim = [dim.split('_')[0]]
        newDim.extend([i.capitalize() for i in dim.split('_')[1:]]) 
        newDim = ''.join(newDim)  
        df_res[dim] = [ele[newDim.split('.')[0]][newDim.split('.')[1]] for ele in data_resp['results']]
        
    df = df_res.copy()    
    dimensions = [ele['name'] for ele in data['dimensions']]
    metrics = [ele['name'] for ele in data['metrics']]
    # Change type of data to correspond to the schema file
    dimAndMetrics = [(ele['name'],ele['type']) for ele in data['dimensions']]
    dimAndMetrics.extend([(ele['name'],ele['type']) for ele in data['metrics']])
    for namex, type in dimAndMetrics : 
        if type == 'INTEGER' :
            type = 'int'
        df = df.astype({namex:type.lower()})        
        
    return df


