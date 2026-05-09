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

processed_zone = f"{config['hdfs_base_path']}/{config['processed_zone']}/"

def create_processed_zone():
    try:
        status = client.status(processed_zone, strict=False)
        if not status:
            print(f"Creating {processed_zone}...", flush=True)
            client.makedirs(processed_zone)
            print("✓ Processed zone created successfully!", flush=True)
        else:
            print(f"✓ {processed_zone} already exists.", flush=True)
    except Exception as e:
        print(f"Error creating processed zone: {e}", flush=True)

if __name__ == '__main__':
    create_processed_zone()
