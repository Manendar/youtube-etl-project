import os
import sys
import shutil
from hdfs import InsecureClient
from scripts.config_loader import load_config
import kaggle

# Load environment-specific config
config = load_config()

print(f"Starting extract script for [{config['environment'].upper()}]...", flush=True)

# Temporary folder inside container to download files
local_temp = '/tmp/youtube_data'

# HDFS destination path
hdfs_path = f"{config['hdfs_base_path']}/{config['raw_zone']}/"

# Create HDFS client
try:
    client = InsecureClient(config['hdfs_namenode_url'], user='hadoop', timeout=30)
    print("HDFS client created successfully", flush=True)
except Exception as e:
    print(f"Failed to create HDFS client: {e}", flush=True)
    sys.exit(1)


def download_from_kaggle():
    """Download dataset from Kaggle to local temp folder"""
    # remove any existing files
    if os.path.exists(local_temp):
        shutil.rmtree(local_temp)
        print(f"Cleaned up old {local_temp}", flush=True)

    print("Authenticating with Kaggle...", flush=True)

    kaggle.api.authenticate()
    print("Authentication successful!", flush=True)

    print(f"Downloading dataset to {local_temp}...", flush=True)
    kaggle.api.dataset_download_files(
        'datasnaek/youtube-new',
        path=local_temp,
        unzip=True
    )
    print("Download complete!", flush=True)


def upload_files(file_list, folder):
    """Upload a list of local files to HDFS"""
    hdfs_folder = f"{hdfs_path}{folder}"

    try:
        client.makedirs(hdfs_folder)
    except:
        pass

    count = 0
    for file in file_list:
        try:
            local_file = os.path.join(local_temp, file)
            hdfs_file = f"{hdfs_folder}/{file}"
            print(f"Uploading {file}...", flush=True)
            client.upload(hdfs_file, local_file, overwrite=True)
            count += 1
            print(f"Uploaded {file}", flush=True)
        except Exception as e:
            print(f"Failed to upload {file}: {e}", flush=True)

    print(f"{count}/{len(file_list)} files uploaded to {hdfs_folder}", flush=True)


def cleanup():
    """Delete temp folder after upload"""
    if os.path.exists(local_temp):
        shutil.rmtree(local_temp)
        print(f"Cleaned up {local_temp}", flush=True)


if __name__ == '__main__':
    # Download from Kaggle
    download_from_kaggle()

    # List downloaded files
    all_files = os.listdir(local_temp)
    category_files = [f for f in all_files if f.endswith('.json')]
    video_files = [f for f in all_files if f.endswith('.csv')]
    print(f"Found {len(category_files)} category files and {len(video_files)} video files", flush=True)

    # Upload to HDFS
    upload_files(category_files, 'category')
    upload_files(video_files, 'video')

    # Cleanup local temp files
    cleanup()

    print("Extract completed!", flush=True)
