import requests
import json
from datetime import datetime

weather_api_url = "https://api.open-meteo.com/v1/forecast"
landing_zone = "/Volumes/weather/raw_data/data_files/weather/"

locations = [
    {"city":"Roma", "latitude": 41.9028, "longitude": 12.4964},
    {"city":"Milano", "latitude": 45.4643, "longitude": 9.1895},
    {"city":"Firenze", "latitude": 43.7696, "longitude": 11.2558},
    {"city":"Torino", "latitude": 45.0703, "longitude": 7.6869}
]

def fetch_weather_data():
    load_time = datetime.now().strftime("%Y%m%d0_%H%M%S")

    for loc in locations:
        params = {
            "latitude": loc['latitude'],
            "longitude": loc['longitude'],
            "current_weather": True
        }

        try:
            # 1.To get the data
            response = requests.get(weather_api_url, params=params)

            # 2. Check for HTTP codes other than 200
            response.raise_for_status()
            data = response.json()

            # 3.Saving data
            record = {
                "city": loc['city'],
                "ingestion_ts": datetime.now().isoformat(),
                "raw_weather_data": data,
                "source": weather_api_url
            }

            file_name = f"{loc['city']}_{load_time}.json"

            dbutils.fs.put(f"{landing_zone}/{file_name}", json.dumps(record), overwrite=True)

        except Exception as e:
            print(f"Error fetching weather data: {e}")

fetch_weather_data()
