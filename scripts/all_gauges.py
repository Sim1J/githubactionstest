import requests
import csv
import os
from pathlib import Path

# Path setup
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data" / "water"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# All gauges with continuous forecasting
GAUGE_LIST_FILE = DATA_DIR / "continuous_forecast_gauges.csv"

# Read gauge IDs from CSV
gauge_ids = []
with open(GAUGE_LIST_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        gauge_ids.append(row["lid"].strip())

print(f"Found {len(gauge_ids)} gauges to process.\n")

for GAUGE_ID in gauge_ids:
    url = f"https://api.water.noaa.gov/nwps/v1/gauges/{GAUGE_ID}/stageflow/forecast"
    print(f"Fetching forecast for {GAUGE_ID}...")

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        forecast_data = response.json()
    except requests.exceptions.RequestException as e:
        continue

    data_points = forecast_data.get("data", [])
    if not data_points:
        continue

    # Extract times and values
    generated_time = data_points[0]["generatedTime"]
    valid_times = [p["validTime"] for p in data_points]
    secondary_values = [p["secondary"] for p in data_points]

    header = ["forecast_day", "time_of_1h"] + [f"{i+1}h" for i in range(len(valid_times))]
    row = [generated_time, valid_times[0]] + secondary_values

    # Save CSV in ../data/water/
    csv_file = DATA_DIR / f"{GAUGE_ID}_forecast.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(row)