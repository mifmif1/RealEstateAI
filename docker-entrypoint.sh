#!/bin/sh
set -e
echo "Running database migrations..."
python -m database.setup
echo "Starting application..."
exec "$@"
