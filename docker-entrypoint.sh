#!/bin/sh
set -e

# Ensure the app (and local Python packages like `database/`) are importable.
cd /app
export PYTHONPATH="/app${PYTHONPATH:+:$PYTHONPATH}"

echo "Running database migrations..."
python -m database.setup
echo "Starting application..."
exec "$@"
