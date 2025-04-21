import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

file_path = "/Volumes/Marc/2025_Marco/PB28/NewPorcessConcurrent/trigger_info.csv"
df = pd.read_csv(file_path)

# Convert 'Start_Time' column to datetime format
df['Start_Time'] = pd.to_datetime(df['Start_Time'], format='%Y-%m-%dT%H:%M:%S.%fZ')

# Create a new column for the month
df['Month'] = df['Start_Time'].dt.to_period('M')

# Group by month and sum the number of triggers
monthly_totals = df.groupby('Month')['Number_of_Triggers'].sum()

# Plotting
fig, ax = plt.subplots(figsize=(12, 6))
ax.bar(monthly_totals.index.astype(str), monthly_totals, color='skyblue')

ax.set_xlabel('Month')
ax.set_ylabel('Total Number of Triggers')
ax.set_title('Total Number of Triggers Each Month')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha="right")

# Show the plot
plt.tight_layout()
plt.show()
