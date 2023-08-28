import json
import re
from pathlib import Path
from typing import Literal, Optional, Protocol, Tuple, Union

import socket
import httplib2
import pandas as pd

import googleapiclient.discovery
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from oauth2client import file as OAuth2File


#To be completed with other Google API
def connect_service(
    service: Literal["sheets"],
    scope: Literal[
        "https://www.googleapis.com/auth/spreadsheets"
    ],
    credentials_path: Union[str, Path] = Path(__file__).parent
    / "credentials"
    / "service_account.json",
    version: str = "v4",
    http: Optional[httplib2.Http] = None,
    discoveryServiceUrl: Optional[Tuple[str]] = None,
) -> googleapiclient.discovery.Resource:
    """Connect to a Google service.

    Args:
        service (Literal["sheets"]): The name of the service to connect to.
        scope (Literal["https://www.googleapis.com/auth/spreadsheets"]): The scope of the service.
        credentials_path (Union[str, Path], optional): The path to the service account credentials file. Defaults to Path(__file__).parent / "credentials" / "service_account.json".
        version (str, optional): The version of the service to connect to. Defaults to "v4".
        http (Optional[httplib2.Http], optional): The HTTP object to use for requests. Defaults to None.
        discoveryServiceUrl (Optional[Tuple[str]], optional): The URL to use for the discovery service. Defaults to None.

    Raises:
        ValueError: If the credentials file is not a valid JSON file or a service account credentials file.
        FileNotFoundError: If the credentials file is not found.

    Returns:
        googleapiclient.discovery.Resource: The service object.
    """
    try:
        if type(credentials_path) == str and '.json' not in credentials_path:
            creds = json.loads(credentials_path)
            #creds = credentials_path
            if creds.get("type") != "service_account":
                    raise ValueError(
                        "The file is not a correct service account credential file (it should contain the field type with value 'service_account')"
                    )
        else :
            with open(credentials_path, "r") as f:
                creds = json.load(f)

            if creds.get("type") != "service_account":
                raise ValueError(
                    "The file is not a correct service account credential file (it should contain the field type with value 'service_account')"
                )
    except json.JSONDecodeError:
        raise ValueError("Service account credentials file should be a valid JSON file.")
    except KeyError:
        raise ValueError(
            "The file is not a correct service account credential file (it should contain the field type with value 'service_account')"
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Service account credentials file not found at {credentials_path}"
        )
    if type(credentials_path) == str and '.json' not in credentials_path:
        creds = Credentials.from_service_account_info(creds, scopes=[scope])
    else :
        creds = Credentials.from_service_account_file(credentials_path, scopes=[scope])
    

    return build(
        service, version, credentials=creds, http=http, discoveryServiceUrl=discoveryServiceUrl
    )

def fetch_sheet(REQUIRED_CONFIG_KEYS, spreadsheet_id: str, sheet_name: str, range: Optional[str] = ""):
    """Fetch a sheet from a Google Sheet.

    Args:
        credentials_path (Union[str, Path], optional): The path to the service account credentials file. Defaults to Path(__file__).parent / "credentials" / "service_account.json".
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet.
        range (Optional[str], optional): The range of the sheet to fetch. Defaults to "".

    Returns:
        pd.DataFrame: The sheet as a pandas DataFrame.
    """

    # Resolves a bug with google sheet API

    socket.setdefaulttimeout(1200)
    service = connect_service('sheets','https://www.googleapis.com/auth/spreadsheets', REQUIRED_CONFIG_KEYS)

    result = (
        service.spreadsheets()  # type: ignore
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range="!".join([sheet_name, range]) if range else sheet_name,
        )
        .execute()
    ).get("values")

    return pd.DataFrame(columns=result[0], data=result[1 : len(result)])

def update_sheet_values(data_output: pd.DataFrame, REQUIRED_CONFIG_KEYS, spreadsheet_id: str, sheet_name: str, range: Optional[str] = ""):

    """Update values in sheet from a Google Sheet.

    Args:
        data_output (pd.DataFrame) : The data to update in the sheet.
        credentials_path (Union[str, Path], optional): The path to the service account credentials file. Defaults to Path(__file__).parent / "credentials" / "service_account.json".
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet.
        range (Optional[str], optional): The range of the sheet to fetch. Defaults to "".
        

    Returns:
        The action to put data in googleSheet.
    """

    # Resolves a bug with google sheet API

    socket.setdefaulttimeout(1200)
    service = connect_service('sheets','https://www.googleapis.com/auth/spreadsheets', REQUIRED_CONFIG_KEYS)

    data = data_output.fillna(0)
    Data = []
    Weight = data.values.tolist()
    ColumnName = list(data.keys())
    Data.append(ColumnName)
    for i in Weight :
        Data.append(i)

    #Delete data befort export
    clear = service.spreadsheets().values().clear(spreadsheetId = spreadsheet_id,
                            range = sheet_name).execute()  
    
    result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=sheet_name,
            valueInputOption = "USER_ENTERED", 
            body = {"values" : Data}).execute()
    
    print(f"{result.get('updatedCells')} cells updated.")
    return result

