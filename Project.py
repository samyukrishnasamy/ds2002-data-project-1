import requests
import csv
import json
import sqlite3


# download file
def download_file(url, destination):
    try:
        response = requests.get(url)
        response.raise_for_status()  # error
        with open(destination, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully: {destination}")
        return destination
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None


# load CSV files
def load_csv(file_path):
    try:
        with open(file_path, mode='r') as file:
            reader = csv.DictReader(file)
            return list(reader)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None


# load JSON files
def load_json(file_path):
    try:
        with open(file_path, mode='r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return None


# fetch data from API
def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # error
        return response.json()  # Assuming the API returns data in JSON format
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


# Function to save data as CSV
def save_as_csv(data, file_path):
    try:
        if len(data) == 0:
            print("No data to write to CSV.")
            return
        keys = data[0].keys()
        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved as CSV to {file_path}")
    except Exception as e:
        print(f"Error saving CSV: {e}")


# Function to save data as JSON
def save_as_json(data, file_path):
    try:
        with open(file_path, mode='w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved as JSON to {file_path}")
    except Exception as e:
        print(f"Error saving JSON: {e}")


# Function to save data to SQLite database
def save_to_sqlite(data, db_file, table_name):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create table if not exists
        columns = ', '.join([f"{key} TEXT" for key in data[0].keys()])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

        # Insert data
        placeholders = ', '.join(['?' for _ in data[0].keys()])
        for row in data:
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", list(row.values()))

        conn.commit()
        conn.close()
        print(f"Data saved to SQLite database: {db_file}, Table: {table_name}")
    except Exception as e:
        print(f"Error saving to SQLite: {e}")


# Function to print data summary
def print_summary(data, title):
    if data is None or len(data) == 0:
        print(f"No data available for {title}.")
        return
    num_records = len(data)
    num_columns = len(data[0].keys()) if num_records > 0 else 0
    print(f"{title} - Records: {num_records}, Columns: {num_columns}")


# Extended ETL processor to handle file and API data sources
def etl_processor(input_source, output_format, add_columns=None, remove_columns=None, save_destination=None,
                  is_api=False):
    # Step 1: Load the input data (from URL, local CSV, JSON, or API)
    if is_api:
        # Fetch data from an API
        data = fetch_data_from_api(input_source)
        if data is None:
            return
    elif input_source.startswith('http'):
        # Remote URL (assumes CSV or JSON files from URLs)
        file_extension = input_source.split('.')[-1]
        local_file = download_file(input_source, f"downloaded_file.{file_extension}")
        if local_file is None:
            return
        if file_extension == 'csv':
            data = load_csv(local_file)
        elif file_extension == 'json':
            data = load_json(local_file)
        else:
            print("Unsupported file format.")
            return
    else:
        # Local file path
        if input_source.endswith('.csv'):
            data = load_csv(input_source)
        elif input_source.endswith('.json'):
            data = load_json(input_source)
        else:
            print("Unsupported file format.")
            return

    # Step 2: Print summary of the input data
    print_summary(data, "Pre-Processing Data Summary")

    # Step 3: Modify columns if required
    if remove_columns:
        data = [{key: row[key] for key in row if key not in remove_columns} for row in data]
    if add_columns:
        for row in data:
            for col, val in add_columns.items():
                row[col] = val

    # Step 4: Convert and save the data in the desired format
    if output_format == 'csv':
        if save_destination is None:
            save_destination = 'output_data.csv'
        save_as_csv(data, save_destination)
    elif output_format == 'json':
        if save_destination is None:
            save_destination = 'output_data.json'
        save_as_json(data, save_destination)
    elif output_format == 'sqlite':
        if save_destination is None:
            save_destination = 'output_data.db'
        save_to_sqlite(data, save_destination, 'processed_data')
    else:
        print("Unsupported output format.")
        return

    # Step 5: Print summary of the post-processing data
    print_summary(data, "Post-Processing Data Summary")


# Example usage
if __name__ == '__main__':
    # Example for local CSV file usage
    input_source = 'squirrel-data.csv'  # Path to local CSV file
    output_format = 'json'  # Desired output format: csv, json, or sqlite
    add_columns = {'source': 'squirrel_census'}  # Add a new column
    remove_columns = ['X', 'Y']  # Remove columns as needed
    save_destination = 'processed_squirrel_data.json'  # Output file destination
    etl_processor(input_source, output_format, add_columns, remove_columns, save_destination)

    # Example for API usage
    api_url = 'https://api.github.com/repos/pandas-dev/pandas/issues'  # Example API (GitHub)
    output_format = 'json'  # Desired output format for API data
    save_destination = 'github_issues.json'  # Destination for saving the API data
    etl_processor(api_url, output_format, add_columns=None, remove_columns=None, save_destination=save_destination,
                  is_api=True)
