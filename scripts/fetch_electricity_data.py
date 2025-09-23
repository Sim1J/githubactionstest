from datetime import date, timedelta
from gridstatusio import GridStatusClient
import os
import pandas as pd
import zipfile
import io
import xml.etree.ElementTree as ET
import requests

iso_dict =  {"NE": "isone_reliability_region_load_forecast", "NY": "nyiso_zonal_load_forecast_hourly", "MISO":"miso_load_forecast_mid_term", "ERCOT_Zone":"ercot_load_forecast_by_forecast_zone", "ERCOT_Weather": "ercot_load_forecast_by_weather_zone", "SPP":"spp_load_forecast_mid_term", "PJM" : "pjm_load_forecast_hourly", "CAISO": "caiso_sld_fcst"}

today = date.today()
year = today.strftime("%Y")
today_date = today.strftime("%Y-%m-%d")
eight_days_date = (today + timedelta(days=8)).strftime("%Y-%m-%d")

client = GridStatusClient("debfe9375e344278b9772738158a53fb")
QUERY_LIMIT = 10_000

for iso_name, api_name in iso_dict.items():
  base_dir = "data"
  category = "electricity"

  if iso_name == "CAISO":
    url = (
          "http://oasis.caiso.com/oasisapi/SingleZip?"
          "queryname=SLD_FCST&market_run_id=7DA&"
          f"startdatetime={today.strftime('%Y%m%dT00:00-0000')}&enddatetime={(today + timedelta(days=7)).strftime('%Y%m%dT00:00-0000')}&version=1"
      )
    resp = requests.get(url)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
      xml_file_name = z.namelist()[0]
      with z.open(xml_file_name) as f:
        xml_data = f.read()

    root = ET.fromstring(xml_data)
    namespace = {'o': 'http://www.caiso.com/soa/OASISReport_v1.xsd'}

    records = []
    for rpt in root.findall('.//o:REPORT_DATA', namespace):
        row = {}
        for item in rpt.findall('.//o:DATA_ITEM', namespace):
            row[item.get('name')] = item.get('value')
        for child in rpt:
            if child.tag.replace(f'{{{namespace["o"]}}}', '') in ['INTERVAL_NUM', 'OPR_DT', 'VALUE']:
                row[child.tag.replace(f'{{{namespace["o"]}}}', '')] = child.text
        records.append(row)

        iso_df = pd.DataFrame(records)
        folder_path = os.path.join(base_dir, category, iso_name, year)
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, "load_forecast.csv")
        iso_df.to_csv(file_path, index=False)
  else:
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

    if iso_name == "SPP":
        file_path = os.path.join(folder_path, "mtlf.csv")
        row = [today, iso_df['interval_start_utc'][0]] + list(iso_df['mtlf'])
        if os.path.exists(file_path):
          pd.DataFrame([row]).to_csv(file_path, mode="a", header=False, index=False)
        else:
          header = ["forecast_day", "time_of_1h"] + [f"{h}h" for h in range(1, len(iso_df['mtlf'])+1)] 
          pd.DataFrame([row], columns=header).to_csv(file_path, index=False)
    elif iso_name == "NE":
        for location in iso_df['location'].unique():
            location_df = iso_df[iso_df['location'] == location]
            safe_location_name = location.replace('.', '').replace('Z', '').replace('-', '_')
            file_path = os.path.join(folder_path, f"{safe_location_name}.csv")
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

