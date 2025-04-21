import os
import csv
from obspy import read
from obspy.signal.filter import highpass
from obspy.signal.trigger import classic_sta_lta, trigger_onset

# Specify the directory on the ejectable disk located at /Volumes/Marco/SeismicData
marco_disk_path = os.path.join('/', 'Volumes', 'Marc', '2025_Marco', 'PB28', 'NewPorcessConcurrent')

# Create a CSV file for storing the trigger information or open an existing file for appending
csv_file_path = os.path.join(marco_disk_path, 'trigger_info.csv')

# Check if the CSV file already exists
csv_exists = os.path.exists(csv_file_path)

# Open the CSV file in append mode
with open(csv_file_path, 'a', newline='') as csvfile:
    # Define the CSV header if the file is newly created
    fieldnames = ['SAC_file', 'Station', 'Start_Time', 'End_Time', 'Number_of_Triggers', 'Trigger_Times']

    # Create a CSV writer
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header to the CSV file if it's a new file
    if not csv_exists:
        csv_writer.writeheader()

    # Loop through each file in the directory
    for filename in os.listdir(marco_disk_path):
        if filename.endswith(".SAC"):
            sac_file_path = os.path.join(marco_disk_path, filename)

            try:
                # Load the SAC file
                stream = read(sac_file_path)

                # Verify that the stream contains at least one trace
                if len(stream) == 0:
                    print(f"Skipping {sac_file_path} as it does not contain any traces.")
                    continue

                # Extract relevant information
                trace = stream[0]
                start_time = trace.stats.starttime
                end_time = trace.stats.endtime

                # Apply high-pass filter at 5 Hz to each trace in the stream
                for tr in stream:
                    tr.data = highpass(tr.data, freq=5, df=tr.stats.sampling_rate, corners=2, zerophase=True)

                # Earthquake detection on the high-pass filtered data
                cft = classic_sta_lta(trace.data, int(1 * trace.stats.sampling_rate), int(10 * trace.stats.sampling_rate))

                # Set trigger levels
                onset_trigger = 8
                end_trigger = 0.5

                # Get trigger onsets
                onset_times = trigger_onset(cft, onset_trigger, end_trigger)

                # Count of triggers
                num_triggers = len(onset_times)

                # Convert trigger times to string for CSV
                trigger_times_str = ', '.join(str(time) for time in onset_times)

                # Write the information to the CSV file
                csv_writer.writerow({
                    'SAC_file': sac_file_path,
                    'Station': trace.stats.station,
                    'Start_Time': start_time,
                    'End_Time': end_time,
                    'Number_of_Triggers': num_triggers,
                    'Trigger_Times': trigger_times_str
                })

            except Exception as e:
                print(f"Skipping {sac_file_path} due to exception: {e}")
                print(f"Data length: {len(trace.data)}")
                print(f"Sampling rate: {trace.stats.sampling_rate}")

# Print a message indicating completion
print(f"Trigger information appended to: {csv_file_path}")