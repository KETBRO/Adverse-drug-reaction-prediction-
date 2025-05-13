import csv
import requests
import os

# Input CSV file containing data
input_file = "FARES_1568_file_download.csv"

# Base URL for constructing the download link
base_url = "https://download.open.fda.gov/drug/event"

# Directory to save downloaded files
output_dir = "downloads"
os.makedirs(output_dir, exist_ok=True)

def construct_url(row):
    """
    Constructs a URL for downloading files based on row data.
    """
    parts = row.split()
    year = parts[0]
    quarter = parts[1].lower()  # Convert 'Q3' to 'q3'
    part_number = int(parts[3])
    total_parts = int(parts[5].strip(")"))  # Extract total parts from '(0032)'

    url = f"{base_url}/{year}{quarter}/drug-event-{part_number:04d}-of-{total_parts:04d}.json.zip"
    return year, quarter, url

def download_file(url, output_path):
    """
    Downloads a file from the given URL and saves it to the specified output path.
    """
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded: {url}")
        else:
            print(f"Failed to download {url}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

# Read the CSV and process each row
with open(input_file, "r") as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        # Ensure row is not empty
        if row:
            data = row[0]
            year, quarter, url = construct_url(data)

            # Create year and quarter subdirectories
            year_dir = os.path.join(output_dir, year)
            os.makedirs(year_dir, exist_ok=True)
            quarter_dir = os.path.join(year_dir, quarter)
            os.makedirs(quarter_dir, exist_ok=True)

            # Construct output path
            file_name = os.path.basename(url)
            output_path = os.path.join(quarter_dir, file_name)

            # Download the file
            download_file(url, output_path)

print("Download process completed.")
