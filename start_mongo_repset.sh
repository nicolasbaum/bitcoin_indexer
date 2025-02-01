#!/bin/bash

# Remove existing data directories (optional)
# Uncomment the next line if you want to start with a clean slate
# rm -rf mongo_repset/rs0

# Create data directory
mkdir -p mongo_repset/rs0

# Set ulimit once
ulimit -n 1048576

# Start mongod instance
mongod --quiet --dbpath mongo_repset/rs0 --replSet rs --port 27017 --bind_ip 127.0.0.1 --logpath mongo_repset/rs0/mongo.log --fork

# Allow time for mongod instance to start
sleep 10

echo "Checking if replica set is already initialized..."
IS_INITIALIZED=$(mongosh --quiet --eval 'rs.status().ok' | grep "1")

if [ -z "$IS_INITIALIZED" ]; then
  echo "Initialising mongo replica set"
  mongosh --quiet --eval 'rs.initiate({
    "_id": "rs",
    "members": [
      {"_id": 0, "host": "localhost:27017", "priority": 1}
    ]
  })'

  MONGO_EXIT_CODE=$(echo $?)
  if [ "$MONGO_EXIT_CODE" != "0" ]; then
    echo "Failed to initialise mongo replica set"
  else
    echo "Replica set initialised successfully"
  fi
else
  echo "Replica set is already initialized."
fi