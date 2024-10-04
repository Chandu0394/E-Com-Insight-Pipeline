import os
import pandas as pd
from src.rogue_record_generator import RogueRecordGenerator
from src.data_cleaner import DataCleaner
from src.upload_to_gcs import GCSUploader

def generate_rogue_records():
    generator = RogueRecordGenerator(num_records=10000)
    df_with_rogue_records = generator.generate_records()
    generator.save_to_csv(df_with_rogue_records, 'data/raw/rogue.csv')  # Save to the correct path
    if os.path.exists('data/raw/rogue.csv'):
        print("Rogue records file created successfully.")
    else:
        print("Error: Rogue records file not found after generation.")

def clean_data():
    try:
        # Load the DataFrame
        df_with_rogue_records = pd.read_csv('data/raw/rogue.csv')  # Ensure correct path

        # Initialize the DataCleaner class with the DataFrame
        cleaner = DataCleaner(df_with_rogue_records)

        # Apply the cleaning steps
        cleaner.perform_eda()\
               .clean_missing_customer_name()\
               .clean_invalid_payment_type()\
               .clean_negative_qty()\
               .clean_datetime_format()

        # Save the cleaned DataFrame to the 'data/cleaned' folder
        cleaner.save_cleaned_data('data/cleaned/cleaned.csv')
        print("Data cleaned and saved successfully.")
    except FileNotFoundError:
        print("Error: The specified file 'rogue.csv' was not found.")  # Corrected filename
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def upload_to_gcs():
    uploader = GCSUploader(
        project_id="batch5",
        service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"
    )

    # Upload the cleaned file
    bucket_name1 = "revbucketgen"
    source_file_name1 = "data/cleaned/cleaned.csv"
    destination_blob_name1 = "cleaned.csv"
    uploader.upload_file(bucket_name1, source_file_name1, destination_blob_name1)

    # Upload the rogue file
    bucket_name2 = "revbucketgen"
    source_file_name2 = "data/raw/rogue.csv"
    destination_blob_name2 = "rogue.csv"
    uploader.upload_file(bucket_name2, source_file_name2, destination_blob_name2)

def main():
    # Menu for selecting the operation
    print("Select the operation you want to perform:")
    print("1. Generate rogue records")
    print("2. Clean data")
    print("3. Upload files to GCS")
    
    try:
        choice = int(input("Enter your choice (1/2/3): "))

        if choice == 1:
            generate_rogue_records()
        elif choice == 2:
            clean_data()
        elif choice == 3:
            upload_to_gcs()
        else:
            print("Invalid choice. Please select a valid option (1/2/3).")
    except ValueError:
        print("Invalid input. Please enter a number (1/2/3).")

if __name__ == "__main__":
    main()
