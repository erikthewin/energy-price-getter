import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Accessing the database connection details
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

import mysql.connector

# Get the next day's date in the required format
def get_next_day_date():
    next_day = datetime.now() + timedelta(days=1)
    return next_day.strftime('%Y/%m-%d')

# Fetch data from the API
def fetch_data():
    base_url = 'https://www.elprisenligenu.dk/api/v1/prices/'
    date_part = get_next_day_date()
    url = f"{base_url}{date_part}_DK2.json"
    
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None
    
# Function to determine DKK_transport_per_kWh based on time of day
def get_transport_rate(time_start):
    # Extract hour from time_start (assuming it's in ISO format)
    hour = datetime.fromisoformat(time_start).hour

    # Define the rate pattern based on the hour of the day
    if 0 <= hour < 6:
        return 0.14  # Lavlast
    elif 6 <= hour < 17:
        return 0.42  # Højlast
    elif 17 <= hour < 21:
        return 1.25  # Spidslast
    elif 21 <= hour < 24:
        return 0.42  # Højlast
    else:
        return 0  # Default case (not really needed for hours 0-23)

# Process and structure the data
def process_data(data, afgift):
    processed_data = []
    
    for entry in data:
        # Get the transport rate based on time of day
        transport_rate = get_transport_rate(entry['time_start'])

        processed_entry = {
            'time_start': entry['time_start'],
            'time_end': entry['time_end'],
            'DKK_per_kWh': round(entry['DKK_per_kWh'] * 1.25, 2),
            'EUR_per_kWh': round(entry['EUR_per_kWh'], 2),
            'EXR': entry['EXR'],
            'DKK_transport_per_kWh': round(transport_rate, 2),  # Example value, adjust as needed
            'DKK_afgift_per_kWh': round(afgift, 2)     # Example value, adjust as needed
        }
        processed_data.append(processed_entry)
    
    return processed_data

# Insert data into MySQL database
def insert_into_db(data):
    # MySQL connection details
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO energy_prices (time_start, time_end, DKK_per_kWh, EUR_per_kWh, EXR, DKK_transport_per_kWh, DKK_afgift_per_kWh)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        DKK_per_kWh = VALUES(DKK_per_kWh),
        EUR_per_kWh = VALUES(EUR_per_kWh),
        EXR = VALUES(EXR),
        DKK_transport_per_kWh = VALUES(DKK_transport_per_kWh),
        DKK_afgift_per_kWh = VALUES(DKK_afgift_per_kWh)
    """

    for entry in data:
        cursor.execute(insert_query, (
            entry['time_start'],
            entry['time_end'],
            entry['DKK_per_kWh'],
            entry['EUR_per_kWh'],
            entry['EXR'],
            entry['DKK_transport_per_kWh'],
            entry['DKK_afgift_per_kWh']
        ))

    conn.commit()
    cursor.close()
    conn.close()

# Main flow
def main():
    afgift = 1.11  # Set this value as needed, as it changes less often

    data = fetch_data()
    if data:
        processed_data = process_data(data, afgift)
        insert_into_db(processed_data)
        print("Data processed and inserted successfully.")
        for entry in processed_data:
            print(entry)

if __name__ == "__main__":
    main()
