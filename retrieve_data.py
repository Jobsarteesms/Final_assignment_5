import requests
import json

def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")

if __name__ == "__main__":
    api_url = "https://github.com/public-apis/public-apis"
    data = fetch_data(api_url)
    with open('data.json', 'w') as f:
        json.dump(data, f)