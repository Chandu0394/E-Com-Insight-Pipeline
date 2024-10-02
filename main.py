
# def main():
#     generator = RogueRecordGenerator(num_records=10000)
#     df_with_rogue_records = generator.generate_records()
#     generator.save_to_csv(df_with_rogue_records, 'rogue.csv')

#     try:
#         df_with_rogue_records = pd.read_csv('data\\raw\\rogue.csv')

#         # Initialize the DataCleaner class with the DataFrame
#         cleaner = DataCleaner(df_with_rogue_records)

#         # Apply the cleaning steps
#         cleaner.clean_missing_customer_name()\
#            .clean_invalid_payment_type()\
#            .clean_unrealistic_price()\
#            .clean_negative_qty()\
#            .clean_datetime_format()

#         # Save the cleaned DataFrame to the 'data/cleaned' folder
#         cleaner.save_cleaned_data('data/cleaned/cleaned.csv')
#     except FileNotFoundError:
#         print("Error: The specified file 'rogue_records.csv' was not found.")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
    
#     uploader = GCSUploader(
#         project_id="batch5",
#         service_account_key_path="C:\\Users\\yeruv\\Downloads\\crack-solstice-432414-k6-c380ea9f8c10.json"
#     )

#     # Upload the first file
#     bucket_name1 = "revprop25"
#     source_file_name1 = "data\\cleaned\\cleaned.csv"
#     destination_blob_name1 = "cleaned.csv"
#     uploader.upload_file(bucket_name1, source_file_name1, destination_blob_name1)

#     # Upload the second file
#     bucket_name2 = "revprop25"
#     source_file_name2 = "data\\raw\\rogue.csv"
#     destination_blob_name2 = "rogue.csv"
#     uploader.upload_file(bucket_name2, source_file_name2, destination_blob_name2)


# if __name__ == "__main__":
#     main()


import os
import pandas as pd
from src.rogue_record_generator import RogueRecordGenerator
from src.data_cleaner import DataCleaner
from src.upload_to_gcs import GCSUploader

def main():
    generator = RogueRecordGenerator(num_records=10000)
    df_with_rogue_records = generator.generate_records()
    generator.save_to_csv(df_with_rogue_records, 'rogue.csv')  # Save to the correct path
    if os.path.exists('data/raw/rogue.csv'):
        print("Rogue records file created successfully.")
    else:
        print("Error: Rogue records file not found after generation.")
  
    try:
        # Load the DataFrame
        df_with_rogue_records = pd.read_csv('data\\raw\\rogue.csv')  # Ensure correct path

        # Initialize the DataCleaner class with the DataFrame
        cleaner = DataCleaner(df_with_rogue_records)

        # Apply the cleaning steps
        cleaner.clean_missing_customer_name()\
               .clean_invalid_payment_type()\
               .clean_unrealistic_price()\
               .clean_negative_qty()\
               .clean_datetime_format()

        # Save the cleaned DataFrame to the 'data/cleaned' folder
        cleaner.save_cleaned_data('data/cleaned/cleaned.csv')
    except FileNotFoundError:
        print("Error: The specified file 'rogue.csv' was not found.")  # Corrected filename
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    #uploading raw and cleaned csv files to gcs
    uploader = GCSUploader(
        project_id="batch5",
        service_account_key_path="C:\\Users\\yeruv\\Downloads\\crack-solstice-432414-k6-c380ea9f8c10.json"
    )

    # Upload the cleaned file
    bucket_name1 = "revprop25"
    source_file_name1 = "data/cleaned/cleaned.csv"
    destination_blob_name1 = "cleaned.csv"
    uploader.upload_file(bucket_name1, source_file_name1, destination_blob_name1)

    # Upload the rogue file
    bucket_name2 = "revprop25"
    source_file_name2 = "data/raw/rogue.csv"
    destination_blob_name2 = "rogue.csv"
    uploader.upload_file(bucket_name2, source_file_name2, destination_blob_name2)


if __name__ == "__main__":
    main()
