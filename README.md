# E-Commerce Data Cleaner & Processor

## Overview

This project focuses on generating synthetic E-Commerce transaction data, cleaning that data, and uploading the cleaned data to Google Cloud Storage (GCS). The project is built using Python and pandas for data handling and Google Cloud SDK for interacting with GCS.

The project consists of three primary components:
1. **Data Generation**: Creates synthetic data for testing.
2. **Data Cleaning**: Cleans the dataset to remove invalid entries and ensures data consistency.
3. **File Upload to GCS**: Uploads cleaned data to a Google Cloud Storage bucket.

## Features

### 1. Data Generation
- Generates synthetic transaction data, including fields like customer name, payment type, price, quantity, and more.
- Automatically saves the generated data to a CSV file for further processing.

### 2. Data Cleaning
- **Clean Missing Values**: Fills missing customer names with "Unknown Customer" and missing failure reasons with "None".
- **Validate Payment Types**: Removes records with invalid payment types such as unsupported payment methods.
- **Cap Unrealistic Prices**: Ensures the price stays within a reasonable range by capping it at a maximum value.
- **Handle Negative Quantities**: Replaces negative quantities with a default value of 1.
- **Datetime Format**: Converts date columns to valid pandas `datetime` format.

### 3. Google Cloud Storage Upload
- Uploads the cleaned dataset to a specified GCS bucket.
- Ensures secure and efficient data upload using Google Cloud SDK.

## Prerequisites

Before using this project, you need the following:
- Python 3.x installed on your machine.
- Google Cloud SDK installed and authenticated (for GCS interaction).
- A GCS bucket to store the cleaned data.



    # def clean_unrealistic_price(self, max_price=10000):
    #    
    #     if 'price' not in self.df.columns:
    #         raise KeyError("The 'price' column is missing in the DataFrame.")
        
    #     self.df.loc[:, 'price'] = pd.to_numeric(self.df['price'], errors='coerce')
    #     self.df.loc[self.df['price'] > max_price, 'price'] = max_price
    #     return self

        # elif issue_type == 'unrealistic_price':
        #     record['price'] = secrets.randbelow(900000) + 100000 






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
        service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-85c847be9fa8.json"
    )

    # Upload the first file
    bucket_name1 = "revbucketgen"
    source_file_name1 = "data\\cleaned\\cleaned.csv"
    destination_blob_name1 = "cleaned.csv"
    uploader.upload_file(bucket_name1, source_file_name1, destination_blob_name1)

    # Upload the second file
    bucket_name2 = "revbucketgen"
    source_file_name2 = "data\\raw\\rogue.csv"
    destination_blob_name2 = "rogue.csv"
    uploader.upload_file(bucket_name2, source_file_name2, destination_blob_name2)
