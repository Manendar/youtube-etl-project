import pandas as pd
import os
import sys
from hdfs import InsecureClient
from scripts.config_loader import load_config

print("Starting transform script...", flush=True)

config = load_config()

try:
    client = InsecureClient(config['hdfs_namenode_url'], user='hadoop', timeout=30)
    print("HDFS client created successfully", flush=True)
except Exception as e:
    print(f"Failed to create HDFS client: {e}", flush=True)
    sys.exit(1)

raw_zone = f"{config['hdfs_base_path']}/{config['raw_zone']}/video/"
staging_zone = f"{config['hdfs_base_path']}/{config['staging_zone']}/"

def clean_and_transform():
    try:
        print(f"Listing files in {raw_zone}...", flush=True)
        files = client.list(raw_zone)
        print(f"Found {len(files)} files in {raw_zone}", flush=True)
    except Exception as e:
        print(f"Error listing files: {e}", flush=True)
        return

    for file in files:
        if file.endswith('.csv'):
            print(f"Processing {file}...", flush=True)
            try:
                local_temp = f'/tmp/{file}'
                print(f"Downloading {file}...", flush=True)
                client.download(f'{raw_zone}{file}', local_temp)

                print(f"Reading CSV {file}...", flush=True)
                df = pd.read_csv(local_temp, lineterminator='\n')
                print(f"Loaded {len(df)} rows from {file}", flush=True)

                df = df.drop_duplicates()
                df = df.dropna(subset=['video_id', 'title'])

                if 'trending_date' in df.columns:
                    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m', errors='coerce')

                if 'publish_time' in df.columns:
                    df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')

                df = df.dropna(subset=['trending_date', 'publish_time'])

                if 'title' in df.columns:
                    df['title'] = df['title'].str.strip()

                if 'description' in df.columns:
                    df['description'] = df['description'].fillna('').str.strip()

                cleaned_file = f'/tmp/cleaned_{file}'
                print(f"Saving cleaned data to {cleaned_file}...", flush=True)
                df.to_csv(cleaned_file, index=False)

                print(f"Uploading {file} to staging zone...", flush=True)
                client.upload(f'{staging_zone}{file}', cleaned_file, overwrite=True)

                os.remove(local_temp)
                os.remove(cleaned_file)

                print(f"Cleaned {file} - {len(df)} records uploaded to staging", flush=True)
            except Exception as e:
                print(f"Error processing {file}: {e}", flush=True)

if __name__ == '__main__':
    clean_and_transform()
    print("Data cleaning completed successfully!", flush=True)
