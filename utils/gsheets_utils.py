import os
import json
from dotenv import load_dotenv
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
import io

load_dotenv(override=True)

# Authorize Google Sheets once
def _get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

    if os.path.exists(creds_json):
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)

    return gspread.authorize(creds)

def get_drive_service():
    scope = ['https://www.googleapis.com/auth/drive']
    creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

    if os.path.exists(creds_json):
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)

    service = build('drive', 'v3', credentials=creds)
    return service

def export_dataframe_to_drive(df, folder_id, filename="export.xlsx"):
    # Save DataFrame locally as Excel
    temp_file_path = f"/tmp/{filename}"
    df.to_excel(temp_file_path, index=False)

    service = get_drive_service()

    file_metadata = {
        'name': filename,
        'parents': [folder_id]  # ID of the folder where you want to upload
    }

    media = MediaFileUpload(temp_file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print(f"Uploaded file ID: {uploaded_file.get('id')}")

    # Clean up local temp file
    os.remove(temp_file_path)
    
def export_dataframe_to_sheet(df, sheet_name, tab_name, clear=True):
    # Export a DataFrame to a specific tab in a Google Sheet.
    client = _get_gspread_client()
    spreadsheet = client.open(sheet_name)

    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="20")

    if clear:
        worksheet.clear()

    set_with_dataframe(worksheet, df)
    print(f"Exported to tab '{tab_name}' in Google Sheet '{sheet_name}'.")
    

def _get_credentials(scopes):
    """
    Load Google service account credentials from environment variable.
    
    Supports three formats:
    1. Absolute path to JSON file
    2. Relative path to JSON file (from this file's directory)
    3. Raw JSON string (for cloud deployments)
    """
    creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS is not set or is empty")

    creds_json = creds_json.strip()

    # Try as absolute or working-directory-relative path
    possible_paths = [
        creds_json,
        os.path.join(os.path.dirname(__file__), creds_json),
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return ServiceAccountCredentials.from_json_keyfile_name(path, scopes)

    # If it starts with '{', try to parse as JSON content
    if creds_json.startswith("{"):
        return ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scopes)

    raise RuntimeError(
        f"GOOGLE_SHEETS_CREDENTIALS is neither a valid path nor JSON. Got: {creds_json!r}"
    )
    
def _get_gspread_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = _get_credentials(scopes)
    return gspread.authorize(creds)

def get_drive_service():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = _get_credentials(scopes)
    return build("drive", "v3", credentials=creds)

def list_files_in_folder(folder_id):
    # List all non-trashed files inside a Google Drive folder.
    service = get_drive_service()

    query = f"'{folder_id}' in parents and trashed = false"
    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()

        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return files

def load_drive_file_as_dataframe(file_id):
    """
    Download a Google Drive file by ID and return it as a pandas DataFrame.
    Supports CSV, Excel, and JSON automatically by detecting mimeType.
    """
    service = get_drive_service()

    # Get file metadata to detect type
    metadata = service.files().get(fileId=file_id, fields="mimeType, name").execute()
    mime = metadata["mimeType"]
    name = metadata["name"]

    # Download file content
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    buffer.seek(0)

    # Auto-detect type
    if mime == "text/csv" or name.lower().endswith(".csv"):
        return pd.read_csv(buffer)

    if mime in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel"
    ] or name.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(buffer)

    if mime == "application/json" or name.lower().endswith(".json"):
        return pd.read_json(buffer)

    raise ValueError(f"Unsupported file type: {mime}")