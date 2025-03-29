import os
import sys
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import io

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def authenticate():
    creds = None
    token_path = "token.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "gd_credentials.json", SCOPES
            )
            creds = flow.run_local_server(
                port=0, access_type="offline", prompt="consent"
            )
            with open(token_path, "w") as token_file:
                token_file.write(creds.to_json())

    return creds


def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    all_files = []
    page_token = None

    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        all_files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return all_files


def download_file(service, file_id, file_name, output_dir):
    request = service.files().get_media(fileId=file_id)
    file_path = output_dir / file_name
    fh = io.FileIO(file_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(
            f"⬇️  Downloading {file_name}: {int(status.progress() * 100)}%",
            end="\r",
        )
    print(f"✅ Downloaded {file_name}")


def main():
    if len(sys.argv) != 2:
        print(
            "Usage: python download_parquets_from_drive.py <local_output_directory>"
        )
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    output_dir.mkdir(parents=True, exist_ok=True)

    folder_id = "14yvUNPVBc3J5rbArL5nEAlxzVs9eNgqk"

    creds = authenticate()
    service = build("drive", "v3", credentials=creds)

    files = list_files_in_folder(service, folder_id)
    if not files:
        print("⚠️ No files found in the specified Google Drive folder.")
        return

    for file in files:
        download_file(service, file["id"], file["name"], output_dir)


main()
