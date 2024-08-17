import json
from pyhive import hive
from hdfs import InsecureClient

def store_data_in_hive(json_file, hdfs_path):
    # Connect to HDFS
    client = InsecureClient('http://namenode_host:port', user='******')
    client.upload(hdfs_path, json_file)

    # Connect to Hive
    conn = hive.Connection(host='hive_host', port=10000, username='******')
    cursor = conn.cursor()

    # Load data into Hive table
    cursor.execute(f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE weather_data")
    conn.close()

if __name__ == "__main__":
    json_file = 'data.json'
    hdfs_path = '/user/hdfs_user/data.json'
    store_data_in_hive(json_file, hdfs_path)