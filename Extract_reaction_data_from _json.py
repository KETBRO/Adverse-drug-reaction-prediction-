import os
import zipfile
import json
import csv

# Directory containing ZIP files
input_dir = "d1"
# Output CSV file
output_file = "adverse_drug_reactions2.csv"

# Fields to extract
fields_to_extract = [
    "patient.drug.medicinalproduct",
    "patient.drug.openfda.generic_name",
    "patient.drug.openfda.substance_name",
    "patient.reaction.reactionmeddrapt", 
    "patient.patientsex",
    "patient.patientonsetage", 
    "patient.patientonsetageunit", 
    "primarysource.reportercountry",
    "occurcountry"
]

def extract_field(data, field_path):
    """
    Extracts a field value from nested JSON data. Handles lists by concatenating items.
    """
    keys = field_path.split(".")
    value = data
    try:
        for key in keys:
            if isinstance(value, list):
                # If it's a list, extract all items and concatenate them
                value = ", ".join(str(item.get(key, "")) for item in value if isinstance(item, dict))
            else:
                value = value.get(key, {})
        return value if isinstance(value, str) else str(value)
    except AttributeError:
        return None  # Return None if a key is missing

def process_zip_file(zip_path, csv_writer, fields):
    """
    Process a single ZIP file: extract, parse JSON, and save structured data.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for json_file in z.namelist():
                if json_file.endswith(".json"):
                    with z.open(json_file) as f:
                        data = json.load(f)
                        for record in data.get("results", []):
                            row = [extract_field(record, field) for field in fields]
                            csv_writer.writerow(row)
        print(f"Processed: {os.path.basename(zip_path)}")
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")

def process_all_zip_files(input_dir, output_csv, fields):
    """
    Process all ZIP files in the input directory and save structured data to a CSV.
    """
    with open(output_csv, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)  # Write the header
        
        # Traverse year and quarter directories
        for year in os.listdir(input_dir):
            year_path = os.path.join(input_dir, year)
            if os.path.isdir(year_path):
                for quarter in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter)
                    if os.path.isdir(quarter_path):
                        for zip_file in os.listdir(quarter_path):
                            if zip_file.endswith(".zip"):
                                zip_path = os.path.join(quarter_path, zip_file)
                                process_zip_file(zip_path, writer, fields)

# Run the script
process_all_zip_files(input_dir, output_file, fields_to_extract)

print(f"Data processing complete. Extracted data saved to {output_file}")
