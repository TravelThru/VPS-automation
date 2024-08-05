from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import pandas as pd
import requests
import bson

# Calculate dates for the last week (Monday to Sunday)
current_datetime = datetime.now()
mon_time = current_datetime - timedelta(days=current_datetime.weekday() + 7)
sun_time = current_datetime - timedelta(days=current_datetime.weekday() + 1)

# MongoDB connection
client = MongoClient('mongodb://DbTeam:1NiIsInR5cCI6IkpXVCJ9@167.86.122.24:5051/?authMechanism=SCRAM-SHA-256&authSource=admin')
db = client['CBS']
collection = db['Clients']

# Query MongoDB
clients_cursor = collection.aggregate([
    {
        '$match': {
            'CreatedAt': {
                '$gte': datetime(mon_time.year, mon_time.month, mon_time.day, 0, 0, 0, tzinfo=timezone.utc),
                '$lt': datetime(sun_time.year, sun_time.month, sun_time.day, 23, 59, 59, tzinfo=timezone.utc)
            }
        }
    },
    {
        '$project': {
            'CreatedAt': 1,
            'FirstName': 1,
            'LastName': 1,
            'Email': 1,
            'DiscountPercentage': 1,
            'Currency': 1
        }
    }
])

# Flatten JSON function
def flatten_json(json_data, parent_key='', sep='.'):
    items = {}
    for key, value in json_data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep=sep))
        else:
            items[new_key] = value
    return items

# Flatten the documents and collect in a list
list_of_flattened_data = []
for document in clients_cursor:
    flattened_data = flatten_json(document)
    # Convert Decimal128 to string
    for key, value in flattened_data.items():
        if isinstance(value, bson.Decimal128):
            flattened_data[key] = str(value)
    list_of_flattened_data.append(flattened_data)

# Create DataFrame
df = pd.DataFrame(list_of_flattened_data)

# Convert CreatedAt to string format
df['CreatedAt'] = pd.to_datetime(df['CreatedAt']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Create Full Name column
df['Full Name'] = df['FirstName'] + " " + df['LastName']

# Reorder columns
df = df[['CreatedAt', 'FirstName', 'LastName', 'Full Name', 'Email', 'DiscountPercentage', 'Currency']]
df.to_excel("client_lisstt.xlsx", index=False)
