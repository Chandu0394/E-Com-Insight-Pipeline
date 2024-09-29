import pandas as pd
import unittest
from src.data_cleaner import DataCleaner

class TestDataCleaner(unittest.TestCase):

    def setUp(self):
        """Set up a sample DataFrame for testing."""
        self.test_data = {
            'customer_name': ['Alice', None, 'Charlie', 'David'],
            'failure_reason': [None, 'None', 'Network Error', None],
            'payment_type': ['Card', 'UPI', 'Invalid Type', 'Wallet'],
            'price': [500, 15000, -200, 1200],
            'qty': [2, -1, 5, 0],
            'datetime': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', 
                         'not a date', '2023-01-03 12:00:00']
        }
        self.df = pd.DataFrame(self.test_data)
        self.cleaner = DataCleaner(self.df)

    def test_clean_missing_customer_name(self):
        """Test cleaning of missing customer names."""
        cleaned_df = self.cleaner.clean_missing_customer_name().get_cleaned_data()
        self.assertEqual(cleaned_df['customer_name'].isnull().sum(), 0)
        self.assertEqual(cleaned_df['customer_name'].iloc[1], 'Unknown Customer')

    def test_clean_invalid_payment_type(self):
        """Test removal of invalid payment types."""
        cleaned_df = self.cleaner.clean_invalid_payment_type().get_cleaned_data()
        self.assertNotIn('Invalid Type', cleaned_df['payment_type'].values)

    def test_clean_unrealistic_price(self):
        """Test capping of unrealistic prices."""
        cleaned_df = self.cleaner.clean_unrealistic_price().get_cleaned_data()
        self.assertTrue((cleaned_df['price'] <= 10000).all())

    def test_clean_negative_qty(self):
        """Test replacement of negative quantities."""
        cleaned_df = self.cleaner.clean_negative_qty().get_cleaned_data()
        self.assertTrue((cleaned_df['qty'] >= 1).all())

    def test_clean_datetime_format(self):
        """Test conversion of datetime to pandas datetime format."""
        cleaned_df = self.cleaner.clean_datetime_format().get_cleaned_data()
        self.assertTrue(pd.to_datetime(cleaned_df['datetime'], errors='coerce').notnull().all())

    def tearDown(self):
        """Clean up after tests."""
        pass  # Add any necessary cleanup code here

if __name__ == '__main__':
    unittest.main()
