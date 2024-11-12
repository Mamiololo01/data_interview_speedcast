import requests
import pandas as pd
from datetime import datetime, timedelta


def extract_data(latitude, longitude, start_date, end_date):
    url = f"https://api.open-meteo.com/v1/forecast"
    params = {
        'end_date': end_date,
        'hourly': ['cloudcover', 'cloudcover_low', 'cloudcover_mid', 'cloudcover_high'],
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'timezone': 'UTC' 
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    # Check for 'hourly' data in response
    if 'hourly' not in data:
        print(f"Error: 'hourly' data not found in the response for coordinates ({latitude}, {longitude})")
        print("Full API response:", data)
        return None
    return data['hourly']

cities = {
    "London": (51.5074, -0.1278),
    "Amsterdam": (52.3676, 4.9041),
    "Lisbon": (38.7223, -9.1393)
}

start_date = "2012-01-01"
end_date = "2022-12-31"

# Set date range within API limits
allowed_start_date = datetime(2012, 1, 1)
allowed_end_date = datetime(2022, 12, 31)

raw_data = []

# Fetch data year by year
for city, (lat, long) in cities.items():
    for year in range(2012, 2022): 
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31" if year < 2024 else allowed_end_date.strftime("%Y-%m-%d")

        data = extract_data(lat, long, start_date, end_date)
        
        # Check if data is None
        if data is not None:
            df = pd.DataFrame(data)
            df['city'] = city
            df['year'] = year
            raw_data.append(df)
        else:
            print(f"No data fetched for {city} in year {year}")

# Combine all data if available
if raw_data:
    all_data = pd.concat(raw_data, ignore_index=True)
    print(all_data.head())
else:
    print("No data available to concatenate.")
    
all_data = pd.concat(raw_data, ignore_index=True)


extract_data(lat, long, start_date, end_date)