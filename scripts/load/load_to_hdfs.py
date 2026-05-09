from hdfs import InsecureClient
import os

client = InsecureClient('http://namenode:9870', user='hadoop')

local_path = '/app/data/'
hdfs_raw_zone = '/app/raw_zone/'

def upload_to_hdfs(file_list, folder):
    """Upload a list of local files to a specific HDFS folder"""
    count = 0
    target_path = f"{hdfs_raw_zone}{folder}"
    client.makedirs(target_path)
    
    for file in file_list:
        try:
            client.upload(f"{target_path}/{file}", f"{local_path}/{file}", overwrite=True)
            count += 1
            print(f"  ✓ Uploaded {file}")
        except Exception as e:
            print(f"  ✗ Failed to upload {file}: {e}")
    
    print(f"{count}/{len(file_list)} files uploaded to {target_path}")

if __name__ == '__main__':
    category_files = [f for f in os.listdir(local_path) if f.endswith('.json')]
    video_files = [f for f in os.listdir(local_path) if f.endswith('.csv')]
    
    upload_to_hdfs(category_files, 'category')
    upload_to_hdfs(video_files, 'video')