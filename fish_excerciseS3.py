import boto3
from pprint import pprint as pp
import csv
import pandas as pd
import pymongo
import io

# Create an client object
s3_client = boto3.client("s3")
bucket_name = "data-eng-resources"
s3_resource = boto3.resource("s3")

# Create obj that contains the contents of the bucket
bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="python")

# Get the specific objects from the bucket list and convert into a dataframe
def get_obj_convertdf(key):
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    dataframe = pd.read_csv(s3_object["Body"])
    return dataframe

# Function that takes in the list of dataframes, combines them and returns the average
def avg(data):
    fish_market = pd.concat(data)
    avg_fish_market = fish_market.groupby("Species").mean(numeric_only=True)
    return avg_fish_market

# Function to combine all the files inside the bucket into one big dataframe
def combined():
    fish_data = []                              # Create empty list to store averages of each df
    for obj in bucket_contents["Contents"]:     # Loop through the buckets contents
        file_key = obj["Key"]                   # Access the file key
        csv1 = file_key.endswith('.csv')        # Check if file is a csv and save result
        if csv1 and 'fish' in file_key:         # Check if file ends with .csv and has 'fish' in it
            df = get_obj_convertdf(file_key)    # Convert into a Dataframe and save
            fish_data.append(df)                # Add each df to list of averages
    return fish_data

# Return the total averages for all the data per specie
avg_fish_data = avg(combined())


# Load the data as a csv into s3
def load_to_s3():
    str_buffer = io.StringIO()
    avg_fish_data.to_csv(str_buffer)
    s3_client.put_object(
        Body=str_buffer.getvalue(),
        Bucket=bucket_name,
        Key='Data26/fish/MariamS.csv'
    )


# Create csv file out of the fish data
def create_csv():
    with open('fishdata.csv', 'w') as csvfile:
        return avg_fish_data.to_csv(csvfile)


# Connect to mongoDB on local machine
def insert_to_local_mongodb():
    client = pymongo.MongoClient()
    db = client['fish']

    db.drop_collection('average_data')
    db.create_collection('average_data')
    file_name = 'fishdata.csv'

    with open(file_name, 'r') as data1:
        for obj in csv.DictReader(data1):
            db.average_data.insert_one(obj)

# Connect to mongodb on the virtual machine
def insert_to_ec2_mongodb():
    client = pymongo.MongoClient("mongodb://18.192.57.167:27017/Sparta")
    db = client['fish']

    db.drop_collection('average_data')
    db.create_collection('average_data')
    file_name = 'fishdata.csv'

    with open(file_name, 'r') as data1:
        for obj in csv.DictReader(data1):
            db.average_data.insert_one(obj)


