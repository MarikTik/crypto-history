import os
import sys
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate():
    creds = None
    token_path = "token.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("gd_credentials.json", SCOPES)
            creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
            with open(token_path, "w") as token_file:
                token_file.write(creds.to_json())

    return creds

def upload_file(service, folder_id: str, local_file_path: Path):
    file_metadata = {
        "name": local_file_path.name,
        "parents": [folder_id]
    }
    media = MediaFileUpload(local_file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"✅ Uploaded {local_file_path.name} to Drive (File ID: {file.get('id')})")

def main():
    if len(sys.argv) != 3:
        print("Usage: python upload_parquets_to_drive.py <directory_path> <drive_folder_id>")
        sys.exit(1)

    local_dir = Path(sys.argv[1])
    folder_id = sys.argv[2]

    if not local_dir.is_dir():
        print(f"❌ Error: {local_dir} is not a valid directory.")
        sys.exit(1)

    creds = authenticate()
    service = build("drive", "v3", credentials=creds)

    parquet_files = list(local_dir.glob("*.parquet"))
    if not parquet_files:
        print("⚠️ No .parquet files found in the given directory.")
        return

    for file in parquet_files:
        upload_file(service, folder_id, file)

if __name__ == "__main__":
    main()