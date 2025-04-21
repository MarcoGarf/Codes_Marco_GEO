import os
import csv
import shutil
import requests
import zipfile
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from obspy import read
from obspy.signal.filter import highpass
from obspy.signal.trigger import classic_sta_lta, trigger_onset

# Constants
BASE_URL = "http://service.iris.edu/fdsnws/dataselect/1/query"
NETWORK = "TX"
STATION = "PB28"
CHANNEL = "HHZ"
FORMAT_TYPE = "sac.zip"
DATA_DIR = "./seismic_data"
CSV_FILE_PATH = "/Volumes/Marc/2025_Marco/PB28/trigger_info.csv"

def get_monthly_folder(date):
    """Returns the folder path for a given month."""
    return os.path.join(DATA_DIR, date.strftime("%Y-%m"))

def check_if_already_processed(start_time):
    """Checks if data for a specific day is already in the CSV."""
    if not os.path.exists(CSV_FILE_PATH):
        return False  # No CSV yet, so nothing is processed
    
    df = pd.read_csv(CSV_FILE_PATH)
    df['Start_Time'] = pd.to_datetime(df['Start_Time'])
    
    return any(df['Start_Time'].dt.date == start_time.date())

def download_monthly_data(start_time, end_time, month_folder):
    """Downloads seismic data for a whole month."""
    current_time = start_time
    while current_time < end_time:
        if check_if_already_processed(current_time):
            print(f"✅ Skipping {current_time.strftime('%Y-%m-%d')} (Already Processed)")
        else:
            next_time = current_time + timedelta(days=1)

            params = {
                'net': NETWORK,
                'sta': STATION,
                'cha': CHANNEL,
                'format': FORMAT_TYPE,
                'starttime': current_time.strftime("%Y-%m-%dT%H:%M:%S"),
                'endtime': next_time.strftime("%Y-%m-%dT%H:%M:%S"),
            }

            try:
                response = requests.get(BASE_URL, params=params, timeout=30)
                if response.status_code == 200:
                    with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                        zip_ref.extractall(month_folder)
                else:
                    print(f"Failed to retrieve data for {current_time}. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {current_time}: {e}")

        current_time += timedelta(days=1)  # Move to next day

def process_seismic_data(month_folder):
    """Processes all seismic data from a month and appends results to CSV."""
    csv_exists = os.path.exists(CSV_FILE_PATH)

    with open(CSV_FILE_PATH, 'a', newline='') as csvfile:
        fieldnames = ['SAC_file', 'Station', 'Start_Time', 'End_Time', 'Number_of_Triggers', 'Trigger_Times']
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not csv_exists:
            csv_writer.writeheader()

        for filename in os.listdir(month_folder):
            if filename.endswith(".SAC"):
                file_path = os.path.join(month_folder, filename)
                try:
                    stream = read(file_path)
                    if len(stream) == 0:
                        continue
                    trace = stream[0]
                    start_time = trace.stats.starttime
                    end_time = trace.stats.endtime

                    # Apply high-pass filter
                    for tr in stream:
                        tr.data = highpass(tr.data, freq=5, df=tr.stats.sampling_rate, corners=2, zerophase=True)

                    # STA/LTA trigger detection
                    cft = classic_sta_lta(trace.data, int(1 * trace.stats.sampling_rate), int(10 * trace.stats.sampling_rate))
                    onset_times = trigger_onset(cft, 8, 0.5)
                    num_triggers = len(onset_times)
                    trigger_times_str = ', '.join(str(time) for time in onset_times)

                    # Append to CSV
                    csv_writer.writerow({
                        'SAC_file': filename,
                        'Station': trace.stats.station,
                        'Start_Time': start_time,
                        'End_Time': end_time,
                        'Number_of_Triggers': num_triggers,
                        'Trigger_Times': trigger_times_str
                    })
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

def generate_monthly_plot():
    """Generates a plot of total triggers per month."""
    df = pd.read_csv(CSV_FILE_PATH)
    df['Start_Time'] = pd.to_datetime(df['Start_Time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
    df['Month'] = df['Start_Time'].dt.to_period('M')
    monthly_totals = df.groupby('Month')['Number_of_Triggers'].sum()

    plt.figure(figsize=(12, 6))
    plt.bar(monthly_totals.index.astype(str), monthly_totals, color='skyblue')
    plt.xlabel('Month')
    plt.ylabel('Total Number of Triggers')
    plt.title('Total Number of Triggers Each Month')
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    start_date = datetime(2021,9, 1)
    end_date = datetime(2025, 2, 25)
    current_month = start_date.replace(day=1)  # Start at the beginning of the month

    while current_month <= end_date:
        next_month = (current_month + timedelta(days=32)).replace(day=1)  # Move to next month
        month_folder = get_monthly_folder(current_month)

        # Ensure the directory is clean for the new month
        if os.path.exists(month_folder):
            shutil.rmtree(month_folder)
        os.makedirs(month_folder, exist_ok=True)

        print(f"\n Downloading data for {current_month.strftime('%Y-%m')}...")
        download_monthly_data(current_month, next_month, month_folder)

        print(f"\nProcessing data for {current_month.strftime('%Y-%m')}...")
        process_seismic_data(month_folder)

        print(f"\n Deleting data for {current_month.strftime('%Y-%m')}...")
        shutil.rmtree(month_folder)  # Delete the processed data

        current_month = next_month  # Move to next month

    print("\nGenerating final plot...")
    generate_monthly_plot()
