import streamlit as st
import pandas as pd
import os
from datetime import datetime
from src.rogue_record_generator import RogueRecordGenerator
from src.data_cleaner import DataCleaner
from src.data_cleaner import get_latest_rogue_csv_file
from src.upload_to_gcs import GCSUploader  # Import the GCSUploader from the upload_to_gcs module

# Title of the app
st.title("E-Com Data Analytics Workflow")

# Button to generate rogue records
if st.button("Generate Rogue Records"):
    try:
        generator = RogueRecordGenerator(num_records=10000, rogue_prob=0.1)
        rogue_df = generator.generate_records()
        
        # Generate the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Create the filename with the timestamp
        filename = f"rogue_{timestamp}.csv"
        # Save to the specified directory
        rogue_df.to_csv(os.path.join('data', 'raw', filename), index=False)
        
        st.success(f"Rogue records generated and saved to 'data/raw/{filename}'")
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Button to clean data
if st.button("Clean Data"):
    try:
        latest_csv_path = get_latest_rogue_csv_file()
        if latest_csv_path:
            df_with_rogue_records = pd.read_csv(latest_csv_path)
            cleaner = DataCleaner(df_with_rogue_records)

            cleaner.perform_eda()\
                   .clean_missing_customer_name()\
                   .clean_invalid_payment_type()\
                   .clean_negative_qty()\
                   .clean_datetime_format()

            cleaned_file_path = 'data/cleaned/cleaned.csv'
            cleaner.save_cleaned_data(cleaned_file_path)
            st.success(f"Data cleaned and saved to '{cleaned_file_path}'")
        else:
            st.warning("No rogue files found to process.")
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Button to upload to GCS
if st.button("Upload to GCS"):
    try:
        uploader = GCSUploader(
            project_id="batch5",  # replace with your project ID
            service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"  # replace with your service account key path
        )
        latest_csv_path, destination_blob_name = uploader.choose_file_to_upload()

        if latest_csv_path:
            bucket_name = "revbucketgen"  # replace with your bucket name
            uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)
            st.success(f"Uploaded {destination_blob_name} to GCS.")
        else:
            st.warning("No files found to upload.")
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Button to run all processes
if st.button("Run All Processes"):
    try:
        generator = RogueRecordGenerator(num_records=10000, rogue_prob=0.1)
        rogue_df = generator.generate_records()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rogue_{timestamp}.csv"
        rogue_df.to_csv(os.path.join('data', 'raw', filename), index=False)

        latest_csv_path = get_latest_rogue_csv_file()
        if latest_csv_path:
            df_with_rogue_records = pd.read_csv(latest_csv_path)
            cleaner = DataCleaner(df_with_rogue_records)

            cleaner.perform_eda()\
                   .clean_missing_customer_name()\
                   .clean_invalid_payment_type()\
                   .clean_negative_qty()\
                   .clean_datetime_format()

            cleaned_file_path = 'data/cleaned/cleaned.csv'
            cleaner.save_cleaned_data(cleaned_file_path)

            uploader = GCSUploader(
                project_id="batch5",  
                service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"  
            )
            latest_csv_path, destination_blob_name = uploader.choose_file_to_upload()
            if latest_csv_path:
                bucket_name = "revbucketgen"  
                uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)

            st.success("All processes completed successfully.")
        else:
            st.warning("No rogue files found to process.")
    except Exception as e:
        st.error(f"Error: {str(e)}")
