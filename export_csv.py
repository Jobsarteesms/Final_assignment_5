import pandas as pd
from pyhive import hive

def export_data_to_csv(hive_query, csv_file):
    # Connect to Hive
    conn = hive.Connection(host='hive_host', port=10000, username='******')
    df = pd.read_sql(hive_query, conn)

    # Save the data to CSV
    df.to_csv(csv_file, index=False)
    conn.close()

if __name__ == "__main__":
    hive_query = "SELECT * FROM weather_data"
    csv_file = 'output.csv'
    export_data_to_csv(hive_query, csv_file)