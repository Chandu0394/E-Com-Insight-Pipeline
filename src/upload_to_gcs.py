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

# Example usage of the class:
if __name__ == "__main__":
    # Instantiate the GCSUploader
    uploader = GCSUploader(
        project_id="batch5",
        service_account_key_path="C:\\Users\\yeruv\\Downloads\\crack-solstice-432414-k6-c380ea9f8c10.json"
    )

    # Upload the first file
    bucket_name1 = "revprop25"
    source_file_name1 = "data\\cleaned\\cleaned.csv"
    destination_blob_name1 = "cleaned.csv"
    uploader.upload_file(bucket_name1, source_file_name1, destination_blob_name1)

    # Upload the second file
    bucket_name2 = "revprop25"
    source_file_name2 = "data\\raw\\rogue.csv"
    destination_blob_name2 = "rogue.csv"
    uploader.upload_file(bucket_name2, source_file_name2, destination_blob_name2)
