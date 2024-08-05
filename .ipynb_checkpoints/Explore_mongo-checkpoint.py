import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import pandas as pd
import numpy as np

# Set date for crawling
current_datetime = datetime.now()
yesterday = current_datetime - timedelta(days=1)

# Map month value to abbreviation
month_mapping = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

month = month_mapping[yesterday.month]
day = str(yesterday.day)

custom_file_name1 = "Booking" + "_" + day + "_" + month
custom_file_name2 = "Request" + "_" + day + "_" + month

tenant_id = 'a3f88450-77ef-4df3-89ea-c69cbc9bc410'
client_id = 'ad6b066a-d749-4f0b-bfbb-bad8de0af5d1'
client_secret = 'YwZ8Q~N6dAwc~sTcMAQsDQXwCKDfPBk81miLVbL4'
site_id = '808e26b7-3730-462a-9c54-6294d85502dd'
drive_id = 'b!tyaOgDA3KkacVGKU2FUC3QBPfrv2tOBNnMl6Al9QzOwPmmMRqTD6QofBvpZgFxgY'

# Endpoints to upload files
upload_url1 = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{custom_file_name1}.csv:/content"
upload_url2 = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{custom_file_name2}.csv:/content"

# Access token
token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default'
}

token_r = requests.post(token_url, data=token_data)
access_token = token_r.json()['access_token']

# Headers
headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/x-www-form-urlencoded',
}

# MongoDB client setup
client = MongoClient('mongodb://DbTeam:1NiIsInR5cCI6IkpXVCJ9@167.86.122.24:5051/?authMechanism=SCRAM-SHA-256&authSource=admin')
database = client['External']
collection = database['BookingDotComRequests']

# Define the date range for querying
start_date = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day, hour=0, minute=0, second=0, tzinfo=timezone.utc)
end_date = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day, hour=23, minute=59, second=0, tzinfo=timezone.utc)

# Define the aggregation pipeline for both result sets
pipeline1 = [
    {
        '$match': {
            'BookingInfo': {'$ne': None},
            'CreatedAt': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$project': {
            'CreatedAt': 1,
            'OriginalRequest.Destination': 1,
            'OriginalRequest.Origin': 1,
            'Region': 1,
            'Country': 1,
            'DistanceInfo.DistanceInMeters': 1,
            'PickupDateTimeUtc': 1,
            'PickupTimezone': 1,
            'BookingInfo.VehicleType': 1,
            'BookingInfo.Price': 1
        }
    }
]

pipeline2 = [
    {
        '$match': {
            'CreatedAt': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$project': {
            'CreatedAt': 1,
            'OriginalRequest.Destination': 1,
            'OriginalRequest.Origin': 1,
            'Region': 1,
            'Country': 1,
            'DistanceInfo.DistanceInMeters': 1,
            'PickupDateTimeUtc': 1,
            'PickupTimezone': 1,
            'BookingInfo.VehicleType': None,
            'BookingInfo.Price': None
        }
    }
]

result1 = collection.aggregate(pipeline1)
result2 = collection.aggregate(pipeline2)

def flatten_json(json_data, parent_key='', sep='.'):
    items = {}
    for key, value in json_data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep=sep))
        else:
            items[new_key] = value
    return items

def process_result(result):
    list_of_flattened_data = []
    for document in result:
        flattened_data = flatten_json(document)
        list_of_flattened_data.append(flattened_data)
    df = pd.DataFrame(list_of_flattened_data)
    df['CreatedAt'] = pd.to_datetime(df['CreatedAt']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['PickupDateTimeUtc'] = pd.to_datetime(df['PickupDateTimeUtc']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['CreatedAt'] = pd.to_datetime(df['CreatedAt'])
    df['Hour_CreatedAt'] = df['CreatedAt'].dt.hour
    df['PickupDateTimeUtc'] = pd.to_datetime(df['PickupDateTimeUtc'])
    df['DistanceInfo.DistanceInKM'] = df['DistanceInfo.DistanceInMeters'] / 1000
    df['Distance.Cat'] = df['DistanceInfo.DistanceInKM'].apply(lambda x: int(x // 10 * 10))
    df["OriginalRequest.Destination.Iata"] = df["OriginalRequest.Destination.Iata"].replace(np.nan, 'null', regex=True)
    df["OriginalRequest.Origin.Iata"] = df["OriginalRequest.Origin.Iata"].replace(np.nan, 'null', regex=True)
    return df

df1 = process_result(result1)
csv_data1 = df1.to_csv(index=False, encoding='utf-8')

df2 = process_result(result2)
df2 = df2.drop_duplicates(keep='first', subset=['OriginalRequest.Origin.Name', 'OriginalRequest.Destination.Name', 'PickupDateTimeUtc', 'Hour_CreatedAt'])
csv_data2 = df2.to_csv(index=False, encoding='utf-8')

upload_headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'text/csv',
}

def upload_file(upload_url, csv_data):
    response = requests.put(upload_url, headers=upload_headers, data=csv_data.encode('utf-8'))
    if response.status_code in [200, 201]:
        print("Upload successful")
    else:
        print(f"Upload failed with status code: {response.status_code}")
        print(response.text)

upload_file(upload_url1, csv_data1)
upload_file(upload_url2, csv_data2)
