from typing import Union, Dict
from io import BytesIO, StringIO
import json
import pandas as pd
import requests
from bson import json_util
import gspread
from google.cloud import storage
from google.oauth2 import service_account
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def read_public_sheets(file_url: str) -> pd.DataFrame:
    """Read a publicly available Google Sheets file as a Pandas Dataframe.

    Parameters
    ----------
    file_url : str
        URL adress to the spreadsheet CSV file.

    Returns
    -------
    pd.DataFrame
        Dataframe loaded from the CSV adress.

    """
    response = requests.get(file_url)
    return pd.read_csv(BytesIO(response.content))


def read_public_file_from_gdrive(
    file_url: str, file_format: str, **kwargs
) -> Union[pd.DataFrame, Dict]:
    """Read public files stored in a Google Drive.

    Parameters
    ----------
    file_url : str
        Google Drive file URL.

    file_format : str
        Type of file format: 'csv', 'xlsx' or 'json'.

    Returns
    -------
    Union[pd.DataFrame, Dict]
        Dataframe (for 'csv' or 'xlsx') or Dictionary ('json') from Google
        Drive file.

    """
    download_url = (
        "https://drive.google.com/uc?export=download&id="
        + file_url.split("/")[-2]
    )

    request_str_io = StringIO(requests.get(download_url).text)

    if file_format == "csv":
        return pd.read_csv(request_str_io, **kwargs)

    elif file_format == "xlsx":
        return pd.read_excel(request_str_io, **kwargs)

    elif file_format == "json":
        return json.load(request_str_io)


def read_private_sheets(
    credentials_json: str, sheet_url: str, worksheet: int = 0
) -> pd.DataFrame:
    """Read a private available Google Sheets as a Pandas Dataframe.

    Parameters
    ----------
    credentials_json : str
        Path to JSON file with GCloud Credentials.

    sheet_url : str
        Spreadheet URL adress.

    worksheet : int (default=0)
        Index or name for the target worksheet.

    Returns
    -------
    pd.DataFrame
        Dataframe loaded from the spreadsheet.

    """
    # Parsing file URL
    file_id = sheet_url.split("/")[-2]

    gcloud = gspread.service_account(filename=credentials_json)
    sheet = gcloud.open_by_key(file_id)
    worksheet = sheet.get_worksheet(worksheet)
    list_rows_worksheet = worksheet.get_all_values()
    return pd.DataFrame(
        list_rows_worksheet[1:], columns=list_rows_worksheet[0]
    )


def read_private_file_from_gdrive(
    file_url: str, file_format: str, google_auth: GoogleAuth, **kwargs
) -> Union[pd.DataFrame, Dict, str]:
    """Read private files from Google Drive.

    Parameters
    ----------
    file_url : str
        URL adress to file in Google Drive.

    file_format : str
        File format can be 'csv', 'xlsx', 'parquet', 'json' or 'txt'.

    google_auth: GoogleAuth
        Google Authentication object with access to target account. For more
        information on how to login using Auth2, please check the link below:
        https://docs.iterative.ai/PyDrive2/quickstart/#authentication

    Returns
    -------
    Union[pd.DataFrame, Dict, str].
        The specified object generate from target file.

    """
    drive = GoogleDrive(google_auth)

    # Parsing file URL
    file_id = file_url.split("/")[-2]

    file = drive.CreateFile({"id": file_id})

    # content_str_io = StringIO(file.GetContentString())
    content_io_buffer = file.GetContentIOBuffer()

    if file_format == "csv":
        return pd.read_csv(
            StringIO(content_io_buffer.read().decode()), **kwargs
        )

    elif file_format == "xlsx":
        return pd.read_excel(content_io_buffer.read(), **kwargs)

    elif file_format == "parquet":
        byte_stream = content_io_buffer.read()
        return pd.read_parquet(BytesIO(byte_stream), **kwargs)

    elif file_format == "json":
        return json.load(StringIO(content_io_buffer.read().decode()))

    elif file_format == "txt":
        byte_stream = content_io_buffer.read()
        return byte_stream.decode("utf-8", **kwargs)


def read_file_from_gcloud_storage(
    file_name: str,
    file_format: str,
    gcp_bucket: str,
    gcp_project: str,
    gcp_credentials_file: str,
    **kwargs,
) -> Union[pd.DataFrame, Dict, str]:
    """Read file from Google Cloud Storage into a specific Python object.

    Parameters
    ----------
    file_name : str
        String with the name of the target file.

    file_format : str
        File format can be 'csv', 'xlsx', 'parquet', 'json' or 'txt'.

    gcp_bucket : str
        String with bucket name.

    gcp_project : str (default="jeitto-datascience")
        String with the name of the project in GCP.

    gcp_credentials_file : str
        Dictionary with GCP credentials.

    Returns
    -------
    Union[pd.DataFrame, Dict, str].
        The specified object generate from target file.

    """
    # Authenticate using gcp json credentials
    credentials = service_account.Credentials.from_service_account_file(
        gcp_credentials_file
    )
    storage_client = storage.Client(
        project=gcp_project, credentials=credentials
    )

    # Define bucket and file to get
    bucket = storage_client.get_bucket(gcp_bucket)
    blob = bucket.get_blob(file_name)
    binary_stream = blob.download_as_string()

    # Return corresponding Python object based on file format
    if file_format == "csv":
        return pd.read_csv(BytesIO(binary_stream), **kwargs)

    elif file_format == "parquet":
        return pd.read_parquet(BytesIO(binary_stream), **kwargs)

    elif file_format == "json":
        return json_util.loads(binary_stream, **kwargs)

    elif file_format == "txt":
        return binary_stream.decode("utf-8", **kwargs)
