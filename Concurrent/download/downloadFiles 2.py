import os
from obspy import read
from datetime import datetime, timedelta
import requests
from io import BytesIO
import zipfile
import time

# URL for seismic waveform data in SAC.zip format
base_url = "http://service.iris.edu/fdsnws/dataselect/1/query"

# Specify the network, station, channel, format, and the desired time range
network = "TX"
station = "PB28"
channel = "HHZ"
format_type = "sac.zip"

# Specify the directory on the ejectable disk located at /Volumes/Marco/SeismicData
marco_disk_path = os.path.join('/', 'Volumes', 'Marco', 'SeismicData','PB28','2024')

# Create the directory if it doesn't exist
os.makedirs(marco_disk_path, exist_ok=True)

# Loop through each day from January 1, 2023, to January 31, 2023
for day in range(1, 32):
    # Construct the date for the current iteration
    current_date = datetime(2022, 3, day, 0, 0, 0)

    # Format the date in the required string format
    current_date_str = current_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Construct the parameters for the request
    params = {
        'net': network,
        'sta': station,
        'cha': channel,
        'format': format_type,
        'starttime': current_date_str,
        'endtime': (current_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # Send HTTP request to get the SAC.zip file
    response = requests.get(base_url, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Unzip the content directly without saving the intermediate ZIP file
        with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
            zip_ref.extractall(marco_disk_path)

        print(f"Downloaded and extracted files for {current_date}")

    else:
        # Print an error message if the request was not successful
        print(f"Failed to retrieve data for {current_date}. Status code: {response.status_code}")

