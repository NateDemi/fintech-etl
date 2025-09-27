#!/bin/bash
set -e

# Get port from environment variable, default to 8080
PORT=${PORT:-8080}

echo "Starting Fintech ETL Service on port $PORT"

# Start the application
exec uvicorn main:app --host 0.0.0.0 --port $PORT
