# ingestion/config.py

import os
from dotenv import load_dotenv

load_dotenv()  

# --- Finnhub ---
FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]
STOCK_SYMBOLS = os.environ["STOCK_SYMBOLS"].split(",")  # string -> liste

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_TRADES = os.environ["KAFKA_TOPIC_TRADES"]
CONSUMER_GROUP = os.environ["CONSUMER_GROUP"]

# --- MinIO / S3 ---
S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET_BRONZE = os.environ["S3_BUCKET_BRONZE"]
S3_BUCKET_SILVER = os.environ["S3_BUCKET_SILVER"]
S3_BUCKET_GOLD = os.environ["S3_BUCKET_GOLD"]

# --- Batching du consumer (Sprint 2) ---
BATCH_SIZE = int(os.environ["BATCH_SIZE"])         # string -> entier
BATCH_TIMEOUT_S = int(os.environ["BATCH_TIMEOUT_S"])