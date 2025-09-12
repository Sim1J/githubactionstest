import os
import requests
from datetime import datetime
import json

CONFIG_PATH = "scripts/config.json"

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def fetch_json(url):
    """Fetch CSV content from API endpoint."""
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text

def json_to_csv(json_data):
  # Ensure we have a list of dicts
    if isinstance(json_data, dict):
        # If top-level dict, wrap in list
        json_data = [json_data]

    keys = sorted(json_data[0].keys())
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(json_data)
    

def save_csv(content, category, site_id, base_dir="data"):
    """Save CSV into /data/category/site/year/month/day.csv"""
    today = datetime.utcnow()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    folder_path = os.path.join(base_dir, category, site_id, year, month)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)
        print(f" Created folder: {folder_path}")
    else:
        print(f" Folder already exists: {folder_path}")

    file_path = os.path.join(folder_path, f"{day}.csv")
    json_to_csv(content, file_path)

    print(f" Saved {category}/{site_id} → {file_path}")

def main():
    config = load_config()

    for category, sites in config.items():
        print(f"\n Fetching {category} data ({len(sites)} sites)...")
        for site in sites:
            site_id = site["id"]
            url = site["url"]
            try:
                print(f"↳ Fetching {category}/{site_id} from {url}")
                json_data = fetch_json(url)
                save_csv(json_data, category, site_id)
            except Exception as e:
                print(f"Failed to fetch {category}/{site_id}: {e}")

    print("\n Finished fetching all categories and sites.")

if __name__ == "__main__":
    main()
