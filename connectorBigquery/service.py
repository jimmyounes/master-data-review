from google.cloud import bigquery
import json
from google.oauth2.credentials import Credentials

def pull_data_from_bigquery_table(query,GOOGLE_APPLICATION_CREDENTIALS,project_id) :
    if '.json' not in GOOGLE_APPLICATION_CREDENTIALS :
        GOOGLE_APPLICATION_CREDENTIALS = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = Credentials.from_authorized_user_info(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/cloud-platform"])        
        client = bigquery.Client(credentials=credentials,project=project_id)
    else :
        client = bigquery.Client() 
    query_job = client.query(query)  # Make an API request.
    df = query_job.result().to_dataframe()
    return df

def create_a_new_biqquery_table(tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id): 
    if '.json' not in GOOGLE_APPLICATION_CREDENTIALS :
        GOOGLE_APPLICATION_CREDENTIALS = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = Credentials.from_authorized_user_info(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/cloud-platform"])        
        client = bigquery.Client(credentials=credentials,project=project_id)
    else :
        client = bigquery.Client() 
    data = tableSchemaFileData
    if {'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :
        data['dimensions'].append({'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'})
        
    if {'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'})
       
    if {'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}) 
    
    schema = [bigquery.SchemaField(k['name'].replace('ga:','').replace('.','_'),k['type'],mode=k['mode']) for x in data.keys() for k in data[x]]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    
def load_table_from_dataframe(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id):
    if '.json' not in GOOGLE_APPLICATION_CREDENTIALS :
        GOOGLE_APPLICATION_CREDENTIALS = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = Credentials.from_authorized_user_info(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/cloud-platform"])        
        client = bigquery.Client(credentials=credentials,project=project_id)
    else :
        client = bigquery.Client() 
    data = tableSchemaFileData
    if {'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :
        data['dimensions'].append({'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'})
        
    if {'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'})
        
            
    if {'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}) 
    
    # [print(k['name'],bigquery.enums.SqlTypeNames[k['type']],k['mode']) for x in data.keys() for k in data[x]]
    # job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField(k['name'],bigquery.enums.SqlTypeNames[k['type']],mode=k['mode']) for x in data.keys() for k in data[x]])
    [print(k['name'].replace('ga:','').replace('.','_'),bigquery.enums.SqlTypeNames[k['type']],k['mode']) for x in data.keys() for k in data[x]]
    job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField(k['name'].replace('ga:','').replace('.','_'),bigquery.enums.SqlTypeNames[k['type']],mode=k['mode']) for x in data.keys() for k in data[x]])
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(table.description)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    
def load_truncate_table_from_dataframe(dataframe,tableSchemaFileData,table_id,GOOGLE_APPLICATION_CREDENTIALS,project_id):
    if '.json' not in GOOGLE_APPLICATION_CREDENTIALS :
        GOOGLE_APPLICATION_CREDENTIALS = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = Credentials.from_authorized_user_info(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/cloud-platform"])        
        client = bigquery.Client(credentials=credentials,project=project_id)
    else :
        client = bigquery.Client() 
    data = tableSchemaFileData
    if {'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :
        data['dimensions'].append({'name': 'clientId', 'type': 'STRING', 'mode': 'NULLABLE'})
        
    if {'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': 'clientName', 'type': 'STRING', 'mode': 'NULLABLE'})
        
    if {'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'} not in data['dimensions'] :    
        data['dimensions'].append({'name': '_extracted_at', 'type': 'DATETIME', 'mode': 'NULLABLE'})  
          
    # [print(k['name'],bigquery.enums.SqlTypeNames[k['type']],k['mode']) for x in data.keys() for k in data[x]]
    # job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField(k['name'],bigquery.enums.SqlTypeNames[k['type']],mode=k['mode']) for x in data.keys() for k in data[x]])
    [print(k['name'].replace('ga:','').replace('.','_'),bigquery.enums.SqlTypeNames[k['type']],k['mode']) for x in data.keys() for k in data[x]]
    job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField(k['name'].replace('ga:','').replace('.','_'),bigquery.enums.SqlTypeNames[k['type']],mode=k['mode']) for x in data.keys() for k in data[x]],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",)
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(table.description)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def export_from_file_to_bq(GOOGLE_APPLICATION_CREDENTIALS, table_id, file_path, project_id) :
    if '.json' not in GOOGLE_APPLICATION_CREDENTIALS :
        GOOGLE_APPLICATION_CREDENTIALS = json.loads(GOOGLE_APPLICATION_CREDENTIALS)
        credentials = Credentials.from_authorized_user_info(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/cloud-platform"])        
        client = bigquery.Client(credentials=credentials,project=project_id)
    else :
        client = bigquery.Client() 
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True #,
            #write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  #added to have truncate and insert load
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

