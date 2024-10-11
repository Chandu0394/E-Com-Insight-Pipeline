import streamlit as st
import pandas as pd
import os
from datetime import datetime
from src.rogue_record_generator import RogueRecordGenerator
from src.data_cleaner import DataCleaner
from src.data_cleaner import get_latest_rogue_csv_file
from src.upload_to_gcs import GCSUploader  # Import the GCSUploader from the upload_to_gcs module

# Title of the app
st.markdown("<h1 style='text-align: center;'>E-Com Data Analytics Workflow</h1>", unsafe_allow_html=True)

# Create a two-column layout for "Run in Steps" and "Run in Batch Mode"
col1, col2 = st.columns(2)

# Run in Steps Section
with col1:
    st.markdown("<h2 style='text-align: center;'>Run in Steps</h2>", unsafe_allow_html=True)

    # Button to reveal input fields for Rogue Record Generator
    if st.button("Generate Rogue Records", key="generate_rogue"):
        st.session_state.show_rogue_input = True

    # Display input fields only after the button is clicked
    if st.session_state.get("show_rogue_input", False):
        num_records = st.number_input("Number of Rogue Records to Generate", min_value=1, value=10000, step=1)
        rogue_prob = st.number_input("Probability of Rogue Records (0.0 to 1.0)", min_value=0.0, max_value=1.0, value=0.1, step=0.01)

        # Button to confirm and generate records after input fields are filled
        if st.button("Confirm and Generate Records", key="confirm_generate_rogue"):
            try:
                generator = RogueRecordGenerator(num_records=num_records, rogue_prob=rogue_prob)
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

    # Row 2: Clean Data
    if st.button("Clean Data", key="clean_data"):
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

# Row 3: Upload to GCS
st.markdown("<br>", unsafe_allow_html=True)  # Add space between buttons

# Show dropdown after clicking the button
if st.button("Upload to GCS", key="upload_to_gcs"):
    st.session_state.show_file_choice = True

# Display the dropdown if the button has been clicked
if st.session_state.get("show_file_choice", False):
    file_choice = st.selectbox("Select which file to upload to GCS", options=["Raw", "Cleaned"])

    # If Raw is selected, provide sub-options
    if file_choice == "Raw":
        raw_file_choice = st.radio("Select how to choose the raw file", options=["Upload latest", "Choose file path"])
        
        # If the user selects "Choose file path," allow them to input a custom file path
        if raw_file_choice == "Choose file path":
            custom_raw_file_path = st.text_input("Enter the file path for the raw file you want to upload:")
        else:
            custom_raw_file_path = None

    # If Cleaned is selected, provide sub-options
    elif file_choice == "Cleaned":
        cleaned_file_choice = st.radio("Select how to choose the cleaned file", options=["Upload latest", "Choose file path"])
        
        # If the user selects "Choose file path," allow them to input a custom file path
        if cleaned_file_choice == "Choose file path":
            custom_cleaned_file_path = st.text_input("Enter the file path for the cleaned file you want to upload:")
        else:
            custom_cleaned_file_path = None

    if st.button("Confirm Upload to GCS", key="confirm_upload_to_gcs"):
        try:
            uploader = GCSUploader(
                project_id="batch5",  # replace with your project ID
                service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"  # replace with your service account key path
            )

            # Determine the file path and name based on the selection
            if file_choice == "Cleaned":
                if cleaned_file_choice == "Upload latest":
                    latest_csv_path = 'data/cleaned/cleaned.csv'  # Assuming cleaned.csv is the file to be uploaded
                    destination_blob_name = f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                elif cleaned_file_choice == "Choose file path" and custom_cleaned_file_path:
                    latest_csv_path = custom_cleaned_file_path
                    destination_blob_name = f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                else:
                    latest_csv_path = None  # No valid file selected

            elif file_choice == "Raw":
                if raw_file_choice == "Upload latest":
                    latest_csv_path = get_latest_rogue_csv_file()  # Retrieve the latest rogue file
                    if latest_csv_path:
                        destination_blob_name = f"rogue_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    else:
                        st.warning("No raw files found to upload.")
                        latest_csv_path = None
                elif raw_file_choice == "Choose file path" and custom_raw_file_path:
                    latest_csv_path = custom_raw_file_path
                    destination_blob_name = f"rogue_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            if latest_csv_path:
                bucket_name = "revbucketgen"  # replace with your bucket name
                uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)
                st.success(f"Uploaded {destination_blob_name} to GCS.")
            else:
                st.warning("No files found to upload.")
        except Exception as e:
            st.error(f"Error: {str(e)}")

    # Add Quit Application Button in Run in Steps Section
    if st.button("Quit Application", key="quit_steps"):
        st.stop()


# Run in Batch Mode Section
with col2:
    st.markdown("<h2 style='text-align: center;'>Run in Batch Mode</h2>", unsafe_allow_html=True)

    # Step 1: Generate rogue records, clean data, and then wait for the file choice before upload
    if st.button("Run All Processes", key="run_all"):
        try:
            # Generate rogue records
            generator = RogueRecordGenerator(num_records=10000, rogue_prob=0.1)
            rogue_df = generator.generate_records()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"rogue_{timestamp}.csv"
            rogue_df.to_csv(os.path.join('data', 'raw', filename), index=False)
            st.success(f"Rogue records generated and saved to 'data/raw/{filename}'")

            # Clean the data
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
                st.success("Data cleaned and saved to 'data/cleaned/cleaned.csv'")

                # Step 2: Prompt to choose which file to upload to GCS
                st.session_state["run_all_process_completed"] = True  # Set a session state flag to indicate the process is done
            else:
                st.warning("No rogue files found to process.")
        except Exception as e:
            st.error(f"Error: {str(e)}")

    # Step 3: Once processing is completed, ask for file upload choice
    if st.session_state.get("run_all_process_completed", False):
        file_choice = st.selectbox("Select which file to upload to GCS", options=["Raw", "Cleaned"], key="batch_file_choice")

        if st.button("Confirm File Upload", key="batch_confirm_upload"):
            try:
                uploader = GCSUploader(
                    project_id="batch5",  # replace with your project ID
                    service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"  # replace with your service account key path
                )

                if file_choice == "Raw":
                    latest_csv_path = get_latest_rogue_csv_file()  # Retrieve the latest rogue file
                    if latest_csv_path:
                        destination_blob_name = f"rogue_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        bucket_name = "revbucketgen"  # replace with your bucket name
                        uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)
                        st.success(f"Uploaded {destination_blob_name} to GCS.")
                    else:
                        st.warning("No raw files found to upload.")

                elif file_choice == "Cleaned":
                    cleaned_file_path = 'data/cleaned/cleaned.csv'  # Assuming cleaned.csv is the file to be uploaded
                    destination_blob_name = f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    bucket_name = "revbucketgen"  # replace with your bucket name
                    uploader.upload_file(bucket_name, cleaned_file_path, destination_blob_name)
                    st.success(f"Uploaded {destination_blob_name} to GCS.")

            except Exception as e:
                st.error(f"Error: {str(e)}")

    # Add Quit Application Button in Run in Batch Mode Section
    if st.button("Quit Application"):
        st.write("Quitting the application...")
        st.write("Please close this tab or window manually.")
        # Use os._exit(0) to close the app programmatically in local dev mode
        os._exit(0)