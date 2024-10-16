import pandas as pd
import random
import os
from datetime import datetime, timedelta
import secrets

# Helper function for generating random dates
def random_date(start, end):
    return start + timedelta(seconds=secrets.randbelow(int((end - start).total_seconds())))

# Class for generating rogue records
class RogueRecordGenerator:
    def __init__(self, num_records=10000, rogue_prob=0.1):
        self.num_records = num_records
        self.rogue_prob = rogue_prob
        self.electronics = [
            'Smartphone', 'Laptop', 'Tablet', 'Smartwatch', 'Bluetooth Speaker', 
            'Headphones', 'Gaming Console', 'Camera', 'Drone', 'External Hard Drive', 
            'USB Flash Drive', 'Smart TV', 'Wireless Router', 'Portable Charger', 
            'Projector', 'Fitness Tracker', 'VR Headset', 'Home Theater System', 
            'Monitor', 'Bluetooth Earbuds'
        ]
        self.stationery = [
            'Pens', 'Pencils', 'Eraser', 'Notebook', 'Stapler', 
            'Paper Clips', 'Markers', 'Highlighters', 'Ruler', 'Glue Stick', 
            'Scissors', 'Sticky Notes', 'Tape', 'Calculator', 'File Folder', 
            'Binder', 'Whiteboard Markers', 'Pencil Sharpener', 'Letter Opener', 'Paper Cutter'
        ]
        self.books = [
            'Fiction', 'Non-fiction', 'Biography', 'Science Fiction', 'Fantasy', 
            'Mystery', 'Historical Fiction', 'Self-Help', 'Cookbooks', 'Comics', 
            'Graphic Novels', 'Poetry', 'Travel Books', 'Thrillers', 'Horror', 
            'Children\'s Books', 'Young Adult', 'Classics', 'Textbooks', 'Memoir'
        ]
        self.clothing = [
            'T-Shirts', 'Jeans', 'Jackets', 'Sweaters', 'Hoodies', 
            'Dresses', 'Shorts', 'Skirts', 'Suits', 'Blouses', 
            'Coats', 'Pants', 'Socks', 'Underwear', 'Swimwear', 
            'Sportswear', 'Nightwear', 'Shoes', 'Scarves', 'Hats'
        ]
        self.home_kitchen = [
            'Cookware', 'Cutlery', 'Plates', 'Mugs', 'Glasses', 
            'Oven Mitts', 'Kitchen Towels', 'Spatulas', 'Mixing Bowls', 'Food Storage Containers', 
            'Blender', 'Microwave', 'Toaster', 'Coffee Maker', 'Dish Rack', 
            'Chopping Board', 'Measuring Cups', 'Kitchen Scale', 'Air Fryer', 'Pressure Cooker'
        ]
        
        self.product_categories = ['Electronics', 'Stationery', 'Books', 'Clothing', 'Home & Kitchen']
        self.payment_types = ['Card', 'Internet Banking', 'UPI', 'Wallet']
        self.countries = ['India', 'USA', 'UK', 'Germany', 'Australia']
        self.cities = {
            'India': ['Mumbai', 'Bengaluru', 'Indore'],
            'USA': ['Boston', 'New York', 'Chicago'],
            'UK': ['London', 'Oxford', 'Manchester'],
            'Germany': ['Berlin', 'Munich', 'Hamburg'],
            'Australia': ['Sydney', 'Melbourne', 'Brisbane']
        }
        self.websites = ['www.amazon.com', 'www.flipkart.com', 'www.ebay.in', 'www.tatacliq.com']

        self.start_date = datetime(2021, 1, 1)
        self.end_date = datetime(2023, 12, 31)

    def _get_product(self, category):
        """Returns a random product based on category."""
        if category == 'Electronics':
            return random.choice(self.electronics)
        elif category == 'Stationery':
            return random.choice(self.stationery)
        elif category == 'Books':
            return random.choice(self.books)
        elif category == 'Clothing':
            return random.choice(self.clothing)
        elif category == 'Home & Kitchen':
            return random.choice(self.home_kitchen)

    def _introduce_rogue_records(self, record):
        """Introduces rogue records with certain issues."""
        issue_type = random.choice([
            'missing_customer_name', 'invalid_payment_type',
            'negative_qty', 'missing_product_id', 'future_order_date'
        ])
        
        if issue_type == 'missing_customer_name':
            record['customer_name'] = ""
        elif issue_type == 'invalid_payment_type':
            record['payment_type'] = "Invalid"
        elif issue_type == 'negative_qty':
            record['qty'] = secrets.randbelow(50) * -1
        elif issue_type == 'future_order_date':
            record['datetime'] = datetime(2023, 1, 1)
        return record

    def generate_records(self):
        """Generates the records, including rogue ones based on the rogue probability."""
        records = []

        for i in range(self.num_records):
            order_id = i + 1
            customer_id = secrets.randbelow(101) + 100
            customer_name = random.choice(['John Smith', 'Mary Jane', 'Joe Smith', 'Neo', 'Trinity'])

            product_category = random.choice(self.product_categories)
            product_name = self._get_product(product_category)
            product_id = secrets.randbelow(101) + 200

            payment_type = random.choice(self.payment_types)
            qty = secrets.randbelow(50) + 1
            price = secrets.randbelow(9996) + 5
            order_datetime = random_date(self.start_date, self.end_date)

            country = random.choice(self.countries)
            city = random.choice(self.cities[country])
            website = random.choice(self.websites)

            payment_txn_id = secrets.randbelow(90000) + 10000
            payment_success = random.choice(['Y', 'N'])
            failure_reason = ''
            if payment_success == 'N':
                failure_reason = random.choice(['Invalid CVV', 'Insufficient Funds', 'Timeout'])

            # Create the initial record
            record = {
                'order_id': order_id, 'customer_id': customer_id, 'customer_name': customer_name, 
                'product_id': product_id, 'product_name': product_name, 'product_category': product_category,
                'payment_type': payment_type, 'qty': qty, 'price': price, 'datetime': order_datetime,
                'country': country, 'city': city, 'ecommerce_website_name': website, 'payment_txn_id': payment_txn_id,
                'payment_txn_success': payment_success, 'failure_reason': failure_reason
            }

            # Introduce rogue record based on the probability
            if random.random() < self.rogue_prob:
                record = self._introduce_rogue_records(record)

            records.append(record)

        return pd.DataFrame(records)

    def save_to_csv(self, df, filename='rogue.csv'):
        """Saves the DataFrame to a CSV file in the 'data/raw' folder with a unique timestamp."""
        try:
            # Define the folder path
            folder_path = os.path.join('data', 'raw')

            # Ensure the folder exists
            os.makedirs(folder_path, exist_ok=True)

            # Append timestamp to filename to ensure it's unique
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_filename = f"{filename.split('.')[0]}_{timestamp}.csv"

            # Save the CSV file in the specified folder
            file_path = os.path.join(folder_path, new_filename)

            df.to_csv(file_path, index=False)

            print(f"File saved successfully as {file_path}")
        except Exception as e:
            print(f"An error occurred while saving the file: {e}")


# Usage
if __name__ == "__main__":
    # Input the number of records and rogue record probability from the user
    num_records = int(input("Enter the number of records to generate: "))
    rogue_prob = float(input("Enter the probability of rogue records (between 0 and 1): "))

    # Create generator instance
    generator = RogueRecordGenerator(num_records=num_records, rogue_prob=rogue_prob)

    # Generate records and save to CSV
    df_with_rogue_records = generator.generate_records()
    generator.save_to_csv(df_with_rogue_records, 'rogue.csv')
