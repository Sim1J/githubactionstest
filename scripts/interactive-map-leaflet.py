import pandas as pd
import json
import gzip
import base64

# Load cities
cities_df = pd.read_csv('scripts/county_weighted_city_with_hydro_final.csv', encoding='utf-8')
cities = []
for idx, row in cities_df.iterrows():
    cities.append({
        "name": f"{row['city']}, {row['state_id']}",
        "lat": row['lat'],
        "lon": row['lng'],
        "koppen": row['Koppen'],
        "url": f"data/weather/{row['state_id']}/{row['city']}_{row['lat']}N_{row['lng']}W/",
        "url1" : f"https://github.com/Sim1J/githubactionstest/tree/main/data/weather/{row['state_id']}/{row['city']}_{row['lat']}N_{row['lng']}W/"
    })

cities_json = json.dumps(cities)

# Load gauges
gauges_df = pd.read_csv('scripts/continuous_forecast_gauges.csv', encoding='utf-8')
gauges = []
for idx, row in gauges_df.iterrows():
    gauges.append({
        "name": f"{row['lid']}, {row['state_id']}",
        "lat": row['lat'],
        "lon": row['lng'],
        "url": f"https://water.noaa.gov/gauges/{row['lid']}",
        "data_url": f"https://github.com/Sim1J/githubactionstest/tree/main/data/water/{row['state_id']}/{row['lid']}_forecast.csv"
    })
gauges_json = json.dumps(gauges)

# Load and compress koppen
with open('scripts/koppen_grid.json', 'r', encoding='utf-8') as f:
    koppen_str = f.read()

compressed = gzip.compress(koppen_str.encode('utf-8'))
koppen_encoded = base64.b64encode(compressed).decode('ascii')

# Read template
with open('scripts/map-template-leaflet.html', 'r', encoding='utf-8') as f:
    template = f.read()

# Substitute data
html = template.replace('CITIES_DATA_PLACEHOLDER', cities_json)
html = html.replace('GAUGES_DATA_PLACEHOLDER', gauges_json)
html = html.replace('KOPPEN_COMPRESSED_PLACEHOLDER', koppen_encoded)

# Save final HTML
with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html)

