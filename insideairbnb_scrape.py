import os
import requests
import logging
from datetime import datetime, timedelta

import gzip
import shutil


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the base URL template
BASE_URL = "https://data.insideairbnb.com/portugal/norte/porto/{date}/{folder}/{table}"

# Directory to save downloaded files
SAVE_DIR = "porto_tables"
os.makedirs(SAVE_DIR, exist_ok=True)

# Dates and tables to download
DATES = ['2024-06-17', '2024-03-18', '2023-12-17', '2023-09-11']
TABLES = {
    'listings': ('listings.csv.gz', 'data'), 
    'reviews': ('reviews.csv.gz', 'data'), 
    'calendar': ('calendar.csv.gz', 'data'),
    'listings_summary': ('listings.csv', 'visualisations'),
    'reviews_summary': ('reviews.csv', 'visualisations'),
    'neighbourhoods': ('neighbourhoods.csv', 'visualisations'),
    'neighbourhoods_geo': ('neighbourhoods.geojson', 'visualisations'),
}

def download_file(url, save_path, retries=3):
    """Download a file from the given URL and save it to the specified path."""
    try:
        logging.info(f"Starting download: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses
        with open(save_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Downloaded successfully: {save_path}")
    except requests.exceptions.RequestException as e:
        if retries > 0:
            logging.warning(f"Retrying download ({retries} left) due to error: {e}")
            download_file(url, save_path, retries - 1)
        else:
            logging.error(f"Failed to download after retries: {url}")
            logging.error(f"Error: {e}")

def format_save_path(table_name, date, table):
    """Format the save path for a downloaded file."""
    file_extension = '.'.join(table.split('.')[1:])
    return os.path.join(SAVE_DIR, f"{table_name}_{date}.{file_extension}")

def download_all_files():
    """Main function to iterate over dates and tables, downloading each file."""
    for date in DATES:
        for table_name, (table, folder) in TABLES.items():
            url = BASE_URL.format(date=date, folder = folder, table=table)
            save_path = format_save_path(table_name, date, table)
            download_file(url, save_path)
    
    logging.info("All files downloaded.")


def extract_all_gz_files(directory):
    """
    Extracts all .gz files in the specified directory.

    :param directory: Directory where .gz files are located
    """
    for filename in os.listdir(directory):
        if filename.endswith('.gz'):
            gz_path = os.path.join(directory, filename)
            extract_to = os.path.join(directory, filename.rstrip('.gz'))

            with gzip.open(gz_path, 'rb') as f_in:
                with open(extract_to, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            print(f"Extracted: {gz_path} to {extract_to}")
