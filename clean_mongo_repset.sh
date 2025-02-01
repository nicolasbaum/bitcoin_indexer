#!/bin/bash

# Define the MongoDB data directory
MONGO_DATA_DIR="mongo_repset"

# Ensure scripts have execution permissions
chmod +x stop_mongo_repset.sh start_mongo_repset.sh

echo "🛑 Stopping MongoDB replica set..."
./stop_mongo_repset.sh

echo "🗑️ Deleting MongoDB data directory: $MONGO_DATA_DIR"
rm -rf $MONGO_DATA_DIR

echo "✅ Cleaned MongoDB replica set storage."

echo "🚀 Restarting MongoDB replica set from scratch..."
./start_mongo_repset.sh

echo "🔄 Running database initializer to set up collections and indexes..."
poetry run python db_initializer.py

echo "🎉 MongoDB replica set has been cleaned, restarted, and initialized."