import os
import json
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

GDRIVE_FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"])

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

creds = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO,
    scopes=SCOPES
)

service = build("drive", "v3", credentials=creds)

backup_dir = Path("backups")
files = sorted(backup_dir.glob("*.csv"))

latest_file = files[-1]

file_metadata = {
    "name": latest_file.name,
    "parents": [GDRIVE_FOLDER_ID]
}

media = MediaFileUpload(str(latest_file), mimetype="text/csv")

service.files().create(
    body=file_metadata,
    media_body=media
).execute()

print("Backup subido a Google Drive")
