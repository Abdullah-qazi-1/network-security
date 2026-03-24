import os
import sys
import json
import certifi
import pandas as pd
import numpy as np
import pymongo
from dotenv import load_dotenv
 
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
 
# Load environment variables from .env file
load_dotenv()
MONGODB_URL = os.getenv("MONGODB_URL")

# Get trusted SSL certificate path for secure MongoDB connection
ca = certifi.where()
 
 
class NetworkDataExtract:
 
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
 
    def csv_to_json_converter(self, file_path):
        try:
            # Read CSV file into a DataFrame
            data = pd.read_csv(file_path)
 
            # Drop the default index column
            data.reset_index(drop=True, inplace=True)
 
            # Convert each row into a JSON object and return as a list
            records = list(json.loads(data.T.to_json()).values())
 
            return records
 
        except Exception as e:
            raise NetworkSecurityException(e, sys)
 
    def insert_data_mongodb(self, records, database, collection):
        try:
            # Store parameters as class variables
            self.database = database
            self.collection = collection
            self.records = records
 
            # Connect to MongoDB Atlas using URL and SSL certificate
            self.mongo_client = pymongo.MongoClient(
                MONGODB_URL,
                tlsCAFile=ca
            )
 
            # Select the target database
            self.database = self.mongo_client[self.database]
 
            # Select the target collection inside the database
            self.collection = self.database[self.collection]
 
            # Insert all records at once
            self.collection.insert_many(self.records)
 
            return len(self.records)
 
        except Exception as e:
            raise NetworkSecurityException(e, sys)
 
 
if __name__ == "__main__":
 
    FILE_PATH = r"D:\MLOOPS PROJECT\Network_Data\phisingData.csv"
    DATABASE = "networksecurity"    #  automatically ban jayega
    COLLECTION = "NetworkData"
 
    # Initialize the ETL pipeline object
    network_obj = NetworkDataExtract()
 
    # Convert CSV data to list of JSON records
    records = network_obj.csv_to_json_converter(file_path=FILE_PATH)
 
    # Insert records into MongoDB and get total count
    no_of_records = network_obj.insert_data_mongodb(
        records,
        DATABASE,
        COLLECTION
    )
 
    print(f"Total records inserted: {no_of_records}")
 