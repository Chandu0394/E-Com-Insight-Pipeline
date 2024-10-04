import os
import re
from datetime import datetime
import pandas as pd

class DataCleaner:
    def __init__(self, df):
        # Ensure the input is a valid DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")
        
        self.df = df.copy()  # Make a copy to avoid modifying the original DataFrame

    def perform_eda(self):
        """Perform basic exploratory data analysis (EDA) on the DataFrame."""
        # Display the first few rows of the DataFrame
        print("\nFirst 5 rows of the DataFrame:")
        print(self.df.head())
        
        # Display a summary of statistics for numerical columns
        print("\nSummary Statistics:")
        print(self.df.describe())
        
        # Display information about the DataFrame including data types and non-null counts
        print("\nDataFrame Information:")
        print(self.df.info())
        return self

    def clean_missing_customer_name(self):
        """Fill missing values for customer_name and failure_reason."""
        if 'customer_name' in self.df.columns:
            self.df.loc[:, 'customer_name'] = self.df['customer_name'].fillna('Unknown Customer')
        else:
            raise KeyError("The 'customer_name' column is missing in the DataFrame.")
        
        if 'failure_reason' in self.df.columns:
            self.df.loc[:, 'failure_reason'] = self.df['failure_reason'].fillna('None')
        else:
            raise KeyError("The 'failure_reason' column is missing in the DataFrame.")
        
        return self

    def clean_invalid_payment_type(self):
        """Remove records with invalid payment types."""
        if 'payment_type' not in self.df.columns:
            raise KeyError("The 'payment_type' column is missing in the DataFrame.")
        
        valid_payment_types = ['Card', 'Internet Banking', 'UPI', 'Wallet']
        self.df = self.df[self.df['payment_type'].isin(valid_payment_types)].copy()
        return self

    def clean_negative_qty(self):
        """Replace negative quantities with a default of 1."""
        if 'qty' not in self.df.columns:
            raise KeyError("The 'qty' column is missing in the DataFrame.")
        
        self.df.loc[:, 'qty'] = pd.to_numeric(self.df['qty'], errors='coerce')
        self.df.loc[self.df['qty'] < 0, 'qty'] = 1
        return self

    def clean_datetime_format(self):
        """Convert 'datetime' column to pandas datetime format."""
        if 'datetime' not in self.df.columns:
            raise KeyError("The 'datetime' column is missing in the DataFrame.")
        
        self.df.loc[:, 'datetime'] = pd.to_datetime(self.df['datetime'], errors='coerce')
        return self

    def get_cleaned_data(self):
        """Return the cleaned DataFrame."""
        return self.df
    def save_cleaned_data(self, file_path_prefix):
        """Save the cleaned DataFrame to a CSV file with a timestamp."""
        try:
            # Ensure the directory exists
            directory = os.path.dirname(file_path_prefix)
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            # Append the current timestamp to the file name
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = f"cleaned_{timestamp}.csv"
            file_path = os.path.join(directory, file_name)
            
            # Save the DataFrame to CSV
            self.df.to_csv(file_path, index=False)
            print(f"Data cleaning completed. Cleaned data saved to '{file_path}'.")
        except Exception as e:
            print(f"An error occurred while saving the file: {e}")

def get_latest_rogue_csv_file(folder_path='data/raw'):
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

# Usage example:
try:
    # Get the path to the most recent rogue CSV file
    latest_csv_path = get_latest_rogue_csv_file()

    if latest_csv_path:
        print(f"Processing the latest rogue file: {latest_csv_path}")

        # Load the DataFrame from the latest rogue CSV
        df_with_rogue_records = pd.read_csv(latest_csv_path)

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
    else:
        print("No rogue files found to process.")

except FileNotFoundError:
    print("Error: The specified file was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")




