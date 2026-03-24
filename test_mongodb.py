import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# .env file load karo
load_dotenv()

# Environment variable se URI uthao
uri = os.getenv("MONGODB_URL")

# Safety check (important 🔥)
if uri is None:
    raise ValueError("MONGODB_URL not found in .env file")

# MongoDB client
client = MongoClient(uri, server_api=ServerApi('1'))

# Connection test
try:
    client.admin.command('ping')
    print("✅ MongoDB connected successfully!")
except Exception as e:
    print("❌ Connection failed:", e)