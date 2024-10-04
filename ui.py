import tkinter as tk
from tkinter import messagebox
import pandas as pd
import os
from datetime import datetime
from src.rogue_record_generator import RogueRecordGenerator
from src.data_cleaner import DataCleaner
from src.data_cleaner import get_latest_rogue_csv_file
from src.upload_to_gcs import GCSUploader  # Import the GCSUploader from the upload_to_gcs module

class DataProcessingApp:
    def __init__(self, master):
        self.master = master
        master.title("Data Processing Application")

        # Buttons for data generation and cleaning
        self.generate_button = tk.Button(master, text="Generate Rogue Records", command=self.generate_records)
        self.generate_button.pack(pady=10)

        self.clean_button = tk.Button(master, text="Clean Data", command=self.clean_data)
        self.clean_button.pack(pady=10)

        self.upload_button = tk.Button(master, text="Upload to GCS", command=self.upload_to_gcs)
        self.upload_button.pack(pady=10)

        # New button for running the entire process
        self.run_all_button = tk.Button(master, text="Run All Processes", command=self.run_all_processes)
        self.run_all_button.pack(pady=10)

        self.quit_button = tk.Button(master, text="Quit", command=master.quit)
        self.quit_button.pack(pady=10)

    def generate_records(self):
        """Method to generate rogue records and save to CSV."""
        try:
            generator = RogueRecordGenerator(num_records=10000, rogue_prob=0.1)
            rogue_df = generator.generate_records()
        
            # Generate the current timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Create the filename with the timestamp
            filename = f"rogue_{timestamp}.csv"
            # Save to the specified directory
            rogue_df.to_csv(os.path.join('data', 'raw', filename), index=False)
        
            messagebox.showinfo("Success", f"Rogue records generated and saved to 'data/raw/{filename}'")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def clean_data(self):
        """Method to clean the latest rogue CSV file."""
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

                cleaner.save_cleaned_data('data/cleaned/cleaned.csv')
                messagebox.showinfo("Success", f"Data cleaned and saved to 'data/cleaned/cleaned.csv'")
            else:
                messagebox.showwarning("Warning", "No rogue files found to process.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def upload_to_gcs(self):
        """Method to upload the latest cleaned or rogue CSV file to GCS."""
        try:
            uploader = GCSUploader(
                project_id="batch5",  # replace with your project ID
                service_account_key_path="C:\\Users\\yeruv\\Downloads\\projectp2-437312-1236b25e88e2.json"  # replace with your service account key path
            )
            latest_csv_path, destination_blob_name = uploader.choose_file_to_upload()

            if latest_csv_path:
                bucket_name = "revbucketgen"  # replace with your bucket name
                uploader.upload_file(bucket_name, latest_csv_path, destination_blob_name)
                messagebox.showinfo("Success", f"Uploaded {destination_blob_name} to GCS.")
            else:
                messagebox.showwarning("Warning", "No files found to upload.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def run_all_processes(self):
        """Method to run all processes: generate records, clean data, and upload to GCS."""
        try:
            self.generate_records()
            self.clean_data()
            self.upload_to_gcs()
            messagebox.showinfo("Success", "All processes completed successfully.")
        except Exception as e:
            messagebox.showerror("Error", str(e))


# Initialize the application
if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessingApp(root)
    root.mainloop()
