#!/bin/bash

# Set the working directory to the project root where idp-with-infinite-vault is located
cd "$(dirname "$0")"

# Export PYTHONPATH to include the current directory for module resolution
export PYTHONPATH=$(pwd)

echo "Starting Celery worker for text extraction..."
python3 -m celery -A text_extraction.celery_app_config.app worker --loglevel=info -Q text_extraction_queue &

echo "Starting Celery beat scheduler for text extraction..."
python3 -m celery -A text_extraction.celery_app_config.app beat --loglevel=info &

echo "Starting Celery worker for data ingestion..."
python3 -m celery -A data_ingestion.worker.app worker --loglevel=info -Q data_ingestion_queue &

echo "Starting FastAPI RAG API app..."
uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# Note: The script runs uvicorn in the foreground. To stop all processes, terminate this script.
