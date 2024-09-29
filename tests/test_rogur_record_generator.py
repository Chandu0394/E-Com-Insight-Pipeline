import pandas as pd
import unittest
from src.rogue_record_generator import RogueRecordGenerator  # Adjust the import according to your project structure

class TestRogueRecordGenerator(unittest.TestCase):

    def setUp(self):
        """Set up parameters for the RogueRecordGenerator."""
        self.generator = RogueRecordGenerator()

    def test_generate_rogue_record(self):
        """Test generation of a single rogue record."""
        rogue_record = self.generator.generate_rogue_record()
        self.assertIsInstance(rogue_record, dict)
        self.assertIn('customer_name', rogue_record)
        self.assertIn('failure_reason', rogue_record)
        self.assertIn('payment_type', rogue_record)
        self.assertIn('price', rogue_record)
        self.assertIn('qty', rogue_record)
        self.assertIn('datetime', rogue_record)

    def test_generate_multiple_rogue_records(self):
        """Test generation of multiple rogue records."""
        num_records = 5
        rogue_records = self.generator.generate_multiple_rogue_records(num_records)
        self.assertEqual(len(rogue_records), num_records)
        
        for record in rogue_records:
            self.assertIsInstance(record, dict)
            self.assertIn('customer_name', record)
            self.assertIn('failure_reason', record)
            self.assertIn('payment_type', record)
            self.assertIn('price', record)
            self.assertIn('qty', record)
            self.assertIn('datetime', record)

    def test_invalid_payment_type_generation(self):
        """Test that rogue records do not generate invalid payment types."""
        rogue_records = self.generator.generate_multiple_rogue_records(10)
        invalid_payment_types = {'Invalid Type', 'Fake Type'}
        
        for record in rogue_records:
            self.assertNotIn(record['payment_type'], invalid_payment_types)

    def test_datetime_format(self):
        """Test that generated datetime is in a valid format."""
        rogue_record = self.generator.generate_rogue_record()
        datetime_value = rogue_record['datetime']
        self.assertIsInstance(pd.to_datetime(datetime_value, errors='coerce'), pd.Timestamp)

    def tearDown(self):
        """Clean up after tests."""
        pass  # Add any necessary cleanup code here

if __name__ == '__main__':
    unittest.main()
