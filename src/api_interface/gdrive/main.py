import os
import time
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv


class gdrive:
    def __init__(self):
        self.drive = self.authenticate_drive()

    def authenticate_drive(self):
        # Load env variables
        load_dotenv()
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        # Calling GoogleAuth will start the authentication process using 'settings.yaml' if available
        gauth = GoogleAuth()
        # Set gauth settings
        gauth.settings = {
            "client_config": {
                "client_id": client_id,
                "client_secret": client_secret
            },
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": "credentials.json",
            "get_refresh_token": True,
            "oath_scope": [
                "https://www.googleapis.com/auth/drive"
            ],
            "auth_params": {
                "access_type": "offline",
                "approval_prompt": "force"
            }
        }

        # Attempt to load saved credentials
        try:
            gauth.LoadCredentialsFile()
            print(f"Loaded Creds from {gauth.settings.get('save_credentials_file')}")
        except Exception as e:
            print(f"Unable to load creds from {gauth.settings.get('save_credentials_file')}\nE: {e}")

        # Attempt initial authentication
        if gauth.credentials is None:
            print("Starting initial authentication:")
            try:
                gauth.LocalWebServerAuth()
            except Exception as e:
                print(f"Unable to initialize authentication!\n E:{e}")
                return None
        elif gauth.access_token_expired:
            print("Gauth access token expired; refreshing...")
            try:
                gauth.refresh()
            except Exception as e:
                print(f"Unable to refresh token!\n E:{e}")
                return None
        else:
            # Case that credentials are valid and can be authorized
            gauth.Authorize()

        gauth.SaveCredentialsFile()
        print("Authentication Successful!")

        drive = GoogleDrive(gauth)
        return drive


    # Returns a list object containing all file metadata from remote
    def get_all_remote_file_data(self) -> list:
        try:
            file_list = self.drive.ListFile({
                'q': "trashed=false",
                'fields': "items(id, title, mimeType, parents, modifiedDate, fileSize)"
            }).GetList()

            return file_list
        except Exception as e:
            print(f"Error: unable to retrieve file list from Drive!\nE: {e}")
            return None


    # Return a single file object which matches the given title and parent_id
    def get_file_by_name(self, title: str, parent_id: str = None):
        # Create search query for API
        query = f"title='{title}' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        # API call to search files
        file_list = self.drive.ListFile({'q': query, 'maxResults': 1}).GetList()
        if file_list:
            print(f"Found existing file on remote: {title} (ID: {file_list[0]['id']})")
            return file_list[0]
        else:
            print(f"File not found on remote: {title}")
            return None


    # returns drive file object if file is successfully uploaded
    def upload_file(self, local_file_path: str, drive_folder_id: str):
        if not os.path.exists(local_file_path):
            print(f"Error: local file {local_file_path} does not exist!")
            return None
        
        filename = os.path.basename(local_file_path)

        try:
            file_metadata = {"title": filename}
            file_metadata["parents"] = [{"id": drive_folder_id}]
            # Create drive file object
            file_obj = self.drive.CreateFile(file_metadata)
            file_obj.SetContentFile(local_file_path)
            # Upload to drive
            file_obj.Upload()
            print(f"Successfully uploaded '{file_obj["title"]}' (ID: {file_obj["id"]})")
            return file_obj
        except Exception as e:
            print(f"Error: unable to upload file!\n E: {e}")
            return None


    # Replace a file on the remote with the updated local file
    # Returns drive file object if file is successfully uploaded
    def update_remote_file(self, local_file_path: str, drive_file_id: str):
        # Check if file exists locally
        if not os.path.exists(local_file_path):
            print(f"Error: Cannot update remote file ({local_file_path}) as it doesn't exist locally!")
            return None
        
        try:
            # Create file meta data dictionary
            filename = os.path.basename(local_file_path)
            file_metadata = {
                "title": filename,
                "id": drive_file_id
            }
            # Create and upload file object
            file_obj = self.drive.CreateFile(file_metadata)
            file_obj.SetContentFile(local_file_path)
            file_obj.Upload()
            return file_obj
        except Exception as e:
            print(f"Error: unable to update remote file {filename} (ID: {drive_file_id})")
            return None


    # Return file object if specified file is downloaded
    def download_file(self, local_file_path: str, drive_file_id: str):
        file_obj = self.drive.CreateFile({"id": drive_file_id})
        try:
            file_obj.GetContentFile(local_file_path)
            print(f"Successfully downloaded file: {local_file_path}")
            return file_obj
        except Exception as e:
            print(f"Unable to download file: {local_file_path}\n E: {e}")
            return None


    

    
