import os
from obspy import read
from datetime import datetime, timedelta
import requests
from io import BytesIO
import zipfile
from multiprocessing import Pool
import time

# URL for seismic waveform data in SAC.zip format
base_url = "http://service.iris.edu/fdsnws/dataselect/1/query"

# Specify the network, station, channel, format, and the desired time range
network = "TX"
station = "PB28"
channel = "HHZ"
format_type = "sac.zip"

# Specify the directory on the ejectable disk located at /Volumes/Marco/SeismicData
marco_disk_path = os.path.join('/', 'Volumes', 'Marc', '2025_Marco', 'PB28', 'NewPorcessConcurrent')

# Create the directory if it doesn't exist
os.makedirs(marco_disk_path, exist_ok=True)

# Define the start and end dates for the range
start_date = datetime(2025, 1, 1, 0, 0, 0)
end_date = datetime(2025, 1, 30, 23, 59, 59)

# Function to download and extract data for a given time
def download_and_extract_data(current_time):
    current_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        'net': network,
        'sta': station,
        'cha': channel,
        'format': format_type,
        'starttime': current_time_str,
        'endtime': (current_time + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
    }

    max_retries = 3  # Number of retries for 413 error
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(base_url, params=params, timeout=30)  # Added timeout to prevent hanging
            if response.status_code == 200:
                with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                    zip_ref.extractall(marco_disk_path)
                print(f"✅ Downloaded and extracted files for {current_time}")
                return f"Success: {current_time}"
            elif response.status_code == 413:
                retries += 1
                print(f"⚠️ Retrying {current_time} (Retry {retries}) due to 413 error...")
                time.sleep(5)  # Wait for a few seconds before retrying
            else:
                print(f"❌ Failed to retrieve data for {current_time}. Status code: {response.status_code}")
                return f"Failed: {current_time} (Status {response.status_code})"
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data for {current_time}: {e}")
            return f"Error: {current_time} ({e})"

    return f"Max retries reached: {current_time}"

if __name__ == "__main__":
    tic = time.time()

    num_processes = 8  # Adjust based on available CPU cores

    # Create time intervals as a list for multiprocessing
    time_interval = timedelta(hours=1)
    time_range = [start_date + i * time_interval for i in range(int((end_date - start_date) / time_interval) + 1)]

    # Use multiprocessing with map()
    with Pool(num_processes) as pool:
        results = pool.map(download_and_extract_data, time_range)

    toc = time.time()
    print(f"✅ Done in {toc - tic:.4f} seconds")

    # Optionally, print summary of results
    print("\nSummary of downloads:")
    for res in results:
        print(res)
