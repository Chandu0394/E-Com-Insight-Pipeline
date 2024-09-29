# E-Commerce Data Cleaner & Processor

## Overview

This project focuses on generating synthetic E-Commerce transaction data, cleaning that data, and uploading the cleaned data to Google Cloud Storage (GCS). The project is built using Python and pandas for data handling and Google Cloud SDK for interacting with GCS.

The project consists of three primary components:
1. **Data Generation**: Creates synthetic data for testing.
2. **Data Cleaning**: Cleans the dataset to remove invalid entries and ensures data consistency.
3. **File Upload to GCS**: Uploads cleaned data to a Google Cloud Storage bucket.

## Features

### 1. Data Generation
- Generates synthetic transaction data, including fields like customer name, payment type, price, quantity, and more.
- Automatically saves the generated data to a CSV file for further processing.

### 2. Data Cleaning
- **Clean Missing Values**: Fills missing customer names with "Unknown Customer" and missing failure reasons with "None".
- **Validate Payment Types**: Removes records with invalid payment types such as unsupported payment methods.
- **Cap Unrealistic Prices**: Ensures the price stays within a reasonable range by capping it at a maximum value.
- **Handle Negative Quantities**: Replaces negative quantities with a default value of 1.
- **Datetime Format**: Converts date columns to valid pandas `datetime` format.

### 3. Google Cloud Storage Upload
- Uploads the cleaned dataset to a specified GCS bucket.
- Ensures secure and efficient data upload using Google Cloud SDK.

## Prerequisites

Before using this project, you need the following:
- Python 3.x installed on your machine.
- Google Cloud SDK installed and authenticated (for GCS interaction).
- A GCS bucket to store the cleaned data.

