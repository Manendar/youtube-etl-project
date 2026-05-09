import sys
from hdfs import InsecureClient
from scripts.config_loader import load_config

# Load environment-specific config
config = load_config()

print(f"Creating HDFS client for [{config['environment'].upper()}]...", flush=True)

try:
    client = InsecureClient(config['hdfs_namenode_url'], user='hadoop', timeout=30)
    print("HDFS client created successfully", flush=True)
except Exception as e:
    print(f"Failed to create HDFS client: {e}", flush=True)
    sys.exit(1)

# Build zone paths from config
base = config['hdfs_base_path']
zones = [
    base,
    f"{base}/{config['raw_zone']}",
    f"{base}/{config['raw_zone']}/video",
    f"{base}/{config['raw_zone']}/category",
    f"{base}/{config['staging_zone']}",
    f"{base}/{config['processed_zone']}"
]

def create_hdfs_dirs():
    for path in zones:
        try:
            status = client.status(path, strict=False)
            if status:
                print(f"Deleting {path}...", flush=True)
                client.delete(path, recursive=True)
        except:
            pass

        try:
            print(f"Creating {path}...", flush=True)
            client.makedirs(path)
            print(f"Created {path}", flush=True)
        except Exception as e:
            print(f"Error creating {path}: {e}", flush=True)

if __name__ == '__main__':
    create_hdfs_dirs()
    print("All HDFS directories ready!", flush=True)
