from datetime import date, timedelta
from gridstatusio import GridStatusClient
import os
import pandas as pd
import zipfile
import io
import xml.etree.ElementTree as ET
import requests
import time

iso_dict = {"NE": "isone_reliability_region_load_forecast", "NY": "nyiso_zonal_load_forecast_hourly", "MISO":"miso_load_forecast_mid_term", "ERCOT_Zone":"ercot_load_forecast_by_forecast_zone", "ERCOT_Weather": "ercot_load_forecast_by_weather_zone", "SPP":"spp_load_forecast_mid_term", "PJM" : "pjm_load_forecast_hourly", "CAISO":"CAISO"}

today = date.today()
year = today.strftime("%Y")
today_date = today.strftime("%Y-%m-%d")
eight_days_date = (today + timedelta(days=8)).strftime("%Y-%m-%d")

today_date_caiso = today.strftime("%Y%m%d")
seven_days_date_caiso = (today + timedelta(days=7)).strftime("%Y%m%d")

client = GridStatusClient("28a896bb48f747b388a830b9922cf065")
QUERY_LIMIT = 10_000

for iso_name, api_name in iso_dict.items():
  time.sleep(2)
  base_dir = "final data?"
  category = "electricity"

  if(iso_name == "ERCOT_Weather" or iso_name == "ERCOT_Zone"):
    folder_path = os.path.join(base_dir, category, "ERCOT", year, iso_name)
  else:
    folder_path = os.path.join(base_dir, category, iso_name, year)
  os.makedirs(folder_path, exist_ok=True)
  if(iso_name == "CAISO"):
    url = (
    f"http://oasis.caiso.com/oasisapi/SingleZip?queryname=SLD_FCST&market_run_id=7DA&startdatetime={today_date_caiso}T00:00-0000&enddatetime={seven_days_date_caiso}T00:00-0000&version=1"
    )

    resp = requests.get(url)
    resp.raise_for_status()

    # Read the zip file from the response content
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        # Assuming there's only one file in the zip and it's an XML file
        xml_file_name = z.namelist()[0]
        with z.open(xml_file_name) as f:
            xml_data = f.read()

    # Parse the XML data
    root = ET.fromstring(xml_data)

    # Define the namespace
    namespace = {'o': 'http://www.caiso.com/soa/OASISReport_v1.xsd'}

    # Extract data
    records = []
    for rpt in root.findall('.//o:REPORT_DATA', namespace):
        row = {}
        for item in rpt.findall('.//o:DATA_ITEM', namespace):
            row[item.get('name')] = item.get('value')
        # Include other relevant keys if needed
        for child in rpt:
          if child.tag.replace(f'{{{namespace["o"]}}}', '') in ['INTERVAL_START_GMT', 'RESOURCE_NAME', 'VALUE']:
            row[child.tag.replace(f'{{{namespace["o"]}}}', '')] = child.text
        records.append(row)

    df = pd.DataFrame(records)

    # Convert 'INTERVAL_START_GMT' to datetime objects
    df["INTERVAL_START_GMT"] = pd.to_datetime(
        df["INTERVAL_START_GMT"], utc=True, errors="coerce"
    )

    # Split the DataFrame by 'RESOURCE_NAME' and save to separate CSVs
    if 'RESOURCE_NAME' in df.columns:
        for resource_name, resource_df in df.groupby('RESOURCE_NAME'):
            # Clean up resource_name for filename
            safe_resource_name = resource_name.replace('/', '_').replace('\\', '_')
            file_path = os.path.join(folder_path, f"{safe_resource_name}.csv")

            # Prepare the row to append
            row = [today.strftime("%Y-%m-%d"), resource_df["INTERVAL_START_GMT"].iloc[0].strftime("%Y-%m-%d %H:%M:%S%z")] + list(resource_df["VALUE"])

            if os.path.exists(file_path):
                # Append without writing header
                pd.DataFrame([row]).to_csv(file_path, mode="a", header=False, index=False)
            else:
                # Create new file with header row
                header = ["forecast_day", "time_of_1h"] + [f"{h}h" for h in range(1, len(resource_df["VALUE"])+1)]
                pd.DataFrame([row], columns=header).to_csv(file_path, index=False)

            print(f"Saved data for resource '{resource_name}' to: {file_path}")
    else:
        file_path = os.path.join(folder_path, "load_forecast.csv")
        df.to_csv(file_path, index=False)
        print(f"CAISO data saved to: {file_path}")
  else:
    iso_df = client.get_dataset(
    dataset=api_name,
    start=today_date,
    end=eight_days_date,
    publish_time="latest",
    timezone="market",
    limit=QUERY_LIMIT
  )
    if iso_name == "SPP":
      file_path = os.path.join(folder_path, "mtlf.csv")
      row = [today, iso_df['interval_start_utc'][0]] + list(iso_df['mtlf'])
      if os.path.exists(file_path):
        pd.DataFrame([row]).to_csv(file_path, mode="a", header=False, index=False)
      else:
        header = ["forecast_day", "time_of_1h"] + [f"{h}h" for h in range(1, len(iso_df['mtlf'])+1)]
        pd.DataFrame([row], columns=header).to_csv(file_path, index=False)
    elif iso_name == "NE":
        for locations in iso_df['location'].unique():
            print(locations)
            location_df = iso_df[iso_df['location'] == locations]
            file_path = os.path.join(folder_path, f"{locations}.csv")
            row = [today, location_df['interval_start_utc'].iloc[0]] + list(location_df['load_forecast'])
            if os.path.exists(file_path):
                pd.DataFrame([row]).to_csv(file_path, mode="a", header=False, index=False)
            else:
                header = ["forecast_day", "time_of_1h"] + [f"{h}h" for h in range(1, len(location_df['load_forecast'])+1)]
                pd.DataFrame([row], columns=header).to_csv(file_path, index=False)
    else:
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
