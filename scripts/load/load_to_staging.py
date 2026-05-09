import sys
from hdfs import InsecureClient
from scripts.config_loader import load_config

print("Creating HDFS client...", flush=True)

config = load_config()

try:
    client = InsecureClient(config['hdfs_namenode_url'], user='hadoop', timeout=30)
    print("HDFS client created successfully", flush=True)
except Exception as e:
    print(f"Failed to create HDFS client: {e}", flush=True)
    sys.exit(1)

staging_zone = f"{config['hdfs_base_path']}/{config['staging_zone']}/"

def create_staging_zone():
    try:
        status = client.status(staging_zone, strict=False)
        if not status:
            print(f"Creating {staging_zone}...", flush=True)
            client.makedirs(staging_zone)
            print("✓ Staging zone created successfully!", flush=True)
        else:
            print(f"✓ {staging_zone} already exists.", flush=True)
    except Exception as e:
        print(f"Error creating staging zone: {e}", flush=True)

if __name__ == '__main__':
    create_staging_zone()
