# BigQuery API
This Python code provides functions to create and load data into a new BigQuery table. The BigQuery is a serverless, highly-scalable, and cost-effective cloud data warehouse provided by Google Cloud Platform. This code uses the BigQuery Python client library to interact with BigQuery API to create and load data into a BigQuery table.

## Functions

### 1. create_a_new_biqquery_table(tableSchemaFile,table_id)
This function takes two parameters:

tableSchemaFile: a path to a JSON file that defines the schema of the table to be created.
table_id: a unique identifier for the table.
The function creates a BigQuery table using the provided schema and table_id. It returns a message stating that the table has been created.

### 2. load_table_from_dataframe(dataframe,tableSchemaFile,table_id)
This function takes three parameters:

dataframe: a pandas DataFrame containing the data to be loaded into the table.
tableSchemaFile: a path to a JSON file that defines the schema of the table.
table_id: a unique identifier for the table.
The function loads the data from the provided DataFrame into the BigQuery table using the specified schema and table_id. It returns a message stating the number of rows and columns that have been loaded.

## Dependencies

This code uses the following dependencies:

- bigquery: a Python client library for interacting with Google BigQuery API.
- json: a Python built-in library for working with JSON files.
- pandas: a Python library for working with data in a tabular format.
Example Usage:

To create a new table and load data into it, you can use the following code:

```shell
import pandas as pd
from google.cloud import bigquery
import json

# create a new table
tableSchemaFile = 'path/to/schema.json' #See the table-schema exemple
table_id = 'my_new_table' #Path to the dataset + new table ID you want to create
create_a_new_biqquery_table(tableSchemaFile,table_id)

# load data into the table
dataframe = pd.read_csv('path/to/data.csv') # Or a dataframe used in file
load_table_from_dataframe(dataframe,tableSchemaFile,table_id)
```

This code assumes that you have already set up a Google Cloud Platform project, enabled the BigQuery API, and set up your authentication credentials.
