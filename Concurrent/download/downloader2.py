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
marco_disk_path = os.path.join('/', 'Volumes', 'Marco', 'SeismicDataTEXAS','PB28','2024')

# Create the directory if it doesn't exist
os.makedirs(marco_disk_path, exist_ok=True)

# Define the start and end dates for the range
start_date = datetime(2023 ,12, 1, 0, 0, 0)
end_date = datetime(2024, 5, 14, 23, 59, 59)

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
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                zip_ref.extractall(marco_disk_path)
            print(f"Downloaded and extracted files for {current_time}")
            break
        elif response.status_code == 413:
            retries += 1
            print(f"Retrying {current_time} (Retry {retries}) due to 413 error...")
            time.sleep(5)  # Wait for a few seconds before retrying
        else:
            print(f"Failed to retrieve data for {current_time}. Status code: {response.status_code}")
            break

if __name__ == "__main__":
    tic = time.time()
    # Specify the number of processes to use (adjust as needed)
    
    num_processes = 8 # we can change this to the desired number of processes

    # Create a multiprocessing pool
    with Pool(num_processes) as pool:
        time_interval = timedelta(hours=1)
        current_time = start_date
        while current_time <= end_date:
            pool.apply_async(download_and_extract_data, (current_time,))
            current_time += time_interval
        
        pool.close()
        pool.join()
        
    toc = time.time()
    print('Done in {:.4f} seconds'.format(toc-tic))
