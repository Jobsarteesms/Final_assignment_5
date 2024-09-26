import requests
import pandas as pd
from pyhive import hive
from hdfs import InsecureClient
import datetime

# Step 1: Fetch data from public API (CoinGecko API in this example)
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {"vs_currency": "usd"}
response = requests.get(url, params=params)
data = response.json()

# Step 2: Parse the data
parsed_data = []
for coin in data:
    parsed_data.append({
        "id": coin['id'],
        "symbol": coin['symbol'],
        "current_price": coin['current_price'],
        "market_cap": coin['market_cap']
    })

# Step 3: Convert to DataFrame and save locally as CSV
df = pd.DataFrame(parsed_data)
local_csv_path = "/home/sathish/Downloads/hadoop/crypto_data.csv"
df.to_csv(local_csv_path, index=False)

# Step 4: Set up HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='sathish')

# Option 1: Overwrite the existing file in HDFS
try:
    print("Attempting to overwrite the existing file in HDFS...")
    hdfs_client.upload('/user/sathish/crypto_data.csv', local_csv_path, overwrite=True)
    print("File successfully overwritten in HDFS at '/user/sathish/crypto_data.csv'.")
except Exception as e:
    print(f"Error overwriting file: {e}")

# Option 2: Upload the file with a unique name (e.g., appending a timestamp)
try:
    # Generate a timestamp to make the filename unique
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_hdfs_path = f'/user/sathish/crypto_data_{timestamp}.csv'

    print("Uploading the file with a unique name in HDFS...")
    hdfs_client.upload(unique_hdfs_path, local_csv_path)
    print(f"File successfully uploaded to HDFS at '{unique_hdfs_path}'.")
except Exception as e:
    print(f"Error uploading file to new path: {e}")

# Step 5: Connect to Hive and load data from HDFS
try:
    print("Connecting to Hive and loading data...")
    conn = hive.Connection(host='localhost', port=10000, username='sathish', database='default')
    cursor = conn.cursor()

    # Create Hive table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_data (
            id STRING, 
            symbol STRING, 
            current_price DOUBLE, 
            market_cap BIGINT
        )
    ''')

    # Load data from HDFS into Hive table
    cursor.execute("LOAD DATA INPATH '/user/sathish/crypto_data.csv' INTO TABLE crypto_data")
    print("Data successfully loaded into Hive.")
    conn.close()
except Exception as e:
    print(f"Error loading data into Hive: {e}")

# Step 6: download data from Hive as CSV
try:
    print("Fetching data from Hive to generate download CSV...")
    conn = hive.Connection(host='localhost', port=10000, username='sathish', database='default')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM crypto_data')
    hive_data = cursor.fetchall()

    # Save the fetched data as CSV for download
    download_csv_path = "/home/sathish/Downloads/hadoop/download_crypto_data.csv"
    columns = ['id', 'symbol', 'current_price', 'market_cap']
    download_df = pd.DataFrame(hive_data, columns=columns)
    download_df.to_csv(download_csv_path, index=False)

    print(f"Data successfully fetched from Hive and saved to CSV at '{download_csv_path}'.")
    conn.close()
except Exception as e:
    print(f"Error fetching or saving data from Hive: {e}")
