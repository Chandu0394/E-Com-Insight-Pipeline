import pandas as pd
import os

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

    def save_cleaned_data(self, file_path):
        """Save the cleaned DataFrame to a CSV file."""
        try:
            # Ensure the directory exists
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            # Save the DataFrame to CSV
            self.df.to_csv(file_path, index=False)
            print(f"Data cleaning completed. Cleaned data saved to '{file_path}'.")
        except Exception as e:
            print(f"An error occurred while saving the file: {e}")

# Usage example:

try:
    # Load the DataFrame
    df_with_rogue_records = pd.read_csv('data\\raw\\rogue.csv')

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
except FileNotFoundError:
    print("Error: The specified file 'rogue.csv' was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
