import os
import re
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

class GCSUploader:
    def __init__(self, project_id, service_account_key_path):
        """
        Initializes the GCSUploader with the project ID and service account key file.

        :param project_id: GCP project ID.
        :param service_account_key_path: Path to the service account key file.
        """
        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
        self.storage_client = storage.Client(credentials=self.credentials, project=self.project_id)

    def upload_file(self, bucket_name, source_file_name, destination_blob_name):
        """
        Uploads a file to the specified GCS bucket.

        :param bucket_name: Name of the GCS bucket.
        :param source_file_name: Path to the file to be uploaded.
        :param destination_blob_name: The destination path in the GCS bucket.
        """
        try:
            # Get the bucket
            bucket = self.storage_client.bucket(bucket_name)

            # Create a new blob and upload the file
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)

            print(f"File {source_file_name} uploaded to {destination_blob_name}.")

        except Exception as e:
            print(f"An error occurred: {e}")

    def get_latest_cleaned_csv_file(self, folder_path='data/cleaned'):
        """Find the latest cleaned CSV file in the given folder based on timestamp."""
        try:
            # List all files in the folder
            files = os.listdir(folder_path)

            # Filter files that match the 'cleaned_YYYYMMDD_HHMMSS.csv' pattern
            cleaned_files = [f for f in files if re.match(r'cleaned_\d{8}_\d{6}\.csv', f)]

            if not cleaned_files:
                raise FileNotFoundError("No cleaned CSV files found in the specified folder.")

            # Sort files by timestamp extracted from the filename
            cleaned_files.sort(
                key=lambda x: datetime.strptime(x.split('_')[1] + x.split('_')[2].split('.')[0], '%Y%m%d%H%M%S'),
                reverse=True
            )

            # Return the most recent cleaned file
            return os.path.join(folder_path, cleaned_files[0])

        except Exception as e:
            print(f"An error occurred while finding the latest cleaned CSV file: {e}")
            return None

    def get_latest_rogue_csv_file(self, folder_path='data/raw'):
        """Find the latest rogue CSV file in the given folder based on timestamp."""
        try:
            # List all files in the folder
            files = os.listdir(folder_path)

            # Filter files that match the 'rogue_YYYYMMDD_HHMMSS.csv' pattern
            rogue_files = [f for f in files if re.match(r'rogue_\d{8}_\d{6}\.csv', f)]

            if not rogue_files:
                raise FileNotFoundError("No rogue CSV files found in the specified folder.")

            # Sort files by timestamp extracted from the filename
            rogue_files.sort(
                key=lambda x: datetime.strptime(x.split('_')[1] + x.split('_')[2].split('.')[0], '%Y%m%d%H%M%S'),
                reverse=True
            )

            # Return the most recent rogue file
            return os.path.join(folder_path, rogue_files[0])

        except Exception as e:
            print(f"An error occurred while finding the latest rogue CSV file: {e}")
            return None

    def choose_file_to_upload(self):
        """Prompt user to choose which file to upload (cleaned, rogue, or both)."""
        choice = input("Do you want to upload a cleaned file (C), rogue file (R), or both (B)? (C/R/B): ").strip().upper()
        
        if choice == 'C':
            return self.get_latest_cleaned_csv_file(), "cleaned.csv"
        elif choice == 'R':
            return self.get_latest_rogue_csv_file(), "rogue.csv"
        elif choice == 'B':
            rogue_file = self.get_latest_rogue_csv_file()
            cleaned_file = self.get_latest_cleaned_csv_file()
            return (cleaned_file, "cleaned.csv"), (rogue_file, "rogue.csv")
        else:
            print("Invalid choice. Please choose 'C' for cleaned, 'R' for rogue, or 'B' for both.")
            return None, None

# Example usage of the class:
if __name__ == "__main__":
    # Instantiate the GCSUploader
    uploader = GCSUploader(
        project_id="batch5",
        service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"
    )

    # Get the user's choice and the corresponding file path(s)
    file_choice = uploader.choose_file_to_upload()

    # Handle single file upload or both files upload
    if isinstance(file_choice, tuple) and isinstance(file_choice[0], tuple):
        # Upload both cleaned and rogue files
        for file_path, destination_blob_name in file_choice:
            if file_path:
                # Upload the selected file
                bucket_name = "revbucketgen"
                uploader.upload_file(bucket_name, file_path, destination_blob_name)
            else:
                print("No files found to upload.")
    else:
        latest_csv_path, destination_blob_name = file_choice
        if latest_csv_path:
            # Upload the selected file
            bucket_name = "revbucketgen"
            uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)
        else:
            print("No files found to upload.")
