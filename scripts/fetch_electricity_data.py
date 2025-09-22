from datetime import date, timedelta
from gridstatusio import GridStatusClient
import os
import pandas as pd

iso_dict = {"NE": "isone_load_forecast_hourly", "NY": "nyiso_zonal_load_forecast_hourly", "MISO":"miso_load_forecast_mid_term", "ERCOT_Zone":"ercot_load_forecast_by_forecast_zone", "ERCOT_Weather": "ercot_load_forecast_by_weather_zone", "SPP":"spp_load_forecast_mid_term", "PJM" : "pjm_load_forecast_hourly"}

today = date.today()
year = today.strftime("%Y")
today_date = today.strftime("%Y-%m-%d")
eight_days_date = (today + timedelta(days=8)).strftime("%Y-%m-%d")

client = GridStatusClient("debfe9375e344278b9772738158a53fb")
QUERY_LIMIT = 10_000

for iso_name, api_name in iso_dict.items():
  base_dir = "data"
  category = "electricity"

  if(iso_name == "ERCOT_Weather" or iso_name == "ERCOT_Zone"):
    folder_path = os.path.join(base_dir, category, "ERCOT", year, iso_name)
  else:
    folder_path = os.path.join(base_dir, category, iso_name, year)
  os.makedirs(folder_path, exist_ok=True)

  iso_df = client.get_dataset(
  dataset=api_name,
  start=today_date,
  end=eight_days_date,
  publish_time="latest",
  timezone="market",
  limit=QUERY_LIMIT
)


  headers = iso_df.columns
  zones = headers[6:]
  for zone in zones:
    file_path = os.path.join(folder_path, f"{zone}.csv")
    row = [today, iso_df['interval_start_utc'][0]] + list(iso_df[zone])
    
    if os.path.exists(file_path):
      pd.DataFrame([row]).to_csv(file_path, mode="a", header=False, index=False)
    else:
      header = ["forecast_day", "time_of_1h"] + [f"{h}h" for h in range(1, len(iso_df[zone])+1)]
      pd.DataFrame([row], columns=header).to_csv(file_path, index=False)

