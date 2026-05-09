# Reads the ENVIRONMENT variable and loads the correct config file.


import os
import yaml
import sys

def load_config():
    # Step 1: Read the ENVIRONMENT variable
    # If not set, default to 'dev' so developers don't break anything accidentally
    env = os.getenv('ENVIRONMENT', 'dev').lower()

    # Step 2: Validate it is one of the 3 allowed environments
    if env not in ['dev', 'test', 'prod']:
        print(f"ERROR: Unknown environment '{env}'")
        print("ENVIRONMENT must be one of: dev, test, prod")
        sys.exit(1)

    # Step 3: Build the path to the correct config file
    # e.g. envs/dev/config.yaml, envs/test/config.yaml, envs/prod/config.yaml
    config_path = f'envs/{env}/config.yaml'

    # Step 4: Check the file exists
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found at {config_path}")
        sys.exit(1)

    # Step 5: Open and parse the YAML file
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    print(f"Loaded [{env.upper()}] config from {config_path}", flush=True)
    return config
