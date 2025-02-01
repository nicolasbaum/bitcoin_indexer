#!/bin/bash

# Define the ports to clear
PORTS=(27017)

# Function to step down the replica set primary
step_down_primary() {
  echo "Attempting to step down the primary node of the replica set..."
  mongosh --port 27017 --eval 'try { rs.stepDown(); } catch (err) { print("Error during stepDown (possibly already stepped down):", err); }'
}

# Function to kill processes using specified ports
kill_processes_on_ports() {
  for PORT in "${PORTS[@]}"; do
    echo "Checking for processes on port $PORT"
    # Find the process IDs (PIDs) using the port
    PIDS=$(lsof -t -i :$PORT)
    if [ -n "$PIDS" ]; then
      echo "Found processes on port $PORT: $PIDS"
      # Terminate the processes
      kill -9 $PIDS
      echo "Terminated processes on port $PORT"
    else
      echo "No processes found on port $PORT"
    fi
  done
}

# Kill any existing mongod processes gracefully (optional)
echo "Terminating any existing mongod processes"

# Step down the replica set primary
step_down_primary

# Stop mongod processes by sending termination signal
echo "Stopping mongod processes gracefully"
pkill -2 mongod
sleep 5  # Allow some time for graceful shutdown

# Kill any remaining processes on the specified ports
echo "Clearing processes on ports ${PORTS[@]}"
kill_processes_on_ports