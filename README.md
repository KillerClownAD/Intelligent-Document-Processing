This project contains components for text extraction, data ingestion, and a Retrieval-Augmented Generation (RAG) pipeline using Celery workers, a beat scheduler, and a FastAPI application.

### RabbitMQ

Using Docker:

```bash
docker run -d --hostname rabbit --name rabbitmq \
  -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Access RabbitMQ Dashboard: http://localhost:15672 (guest/guest)

Or install on Ubuntu:

```bash
sudo apt install rabbitmq-server
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server
```

## Running the Application

Use the following commands to run the Celery workers, beat scheduler, and the RAG API app for the respective modules:

### Text Extraction Module

Run the Celery worker for text extraction queue:
```
python3 -m celery -A text_extraction.celery_app_config.app worker --loglevel=info -Q text_extraction_queue
```

Run the Celery beat scheduler for text extraction:
```
python3 -m celery -A text_extraction.celery_app_config.app beat --loglevel=info
```

### Data Ingestion Module

Run the Celery worker for data ingestion queue:
```
python3 -m celery -A data_ingestion.worker.app worker --loglevel=info -Q data_ingestion_queue
```

### RAG API Application

The RAG API is built with FastAPI and provides endpoints to query the RAG pipeline and retrieve chat history.

Run the FastAPI app with:
```
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

## RAG Pipeline Overview

The RAG pipeline performs the following steps:
- Embeds the user query using an embedding model.
- Retrieves relevant document chunks from ChromaDB.
- Reranks the chunks based on relevance.
- Calls a language model to generate an answer based on the context and chat history.
- Stores the query and answer in MongoDB for session history.

## Notes

- The files `test_rag.py` and `rag_query_pipeline.py` are primarily for testing and development purposes.
- For production, use the API exposed by `app.py` to interact with the RAG pipeline.
- For clearing Python cache, deleting data from MongoDB, or other maintenance tasks, please refer to the existing project documentation or scripts.

## Running the Entire Application

You can use the provided shell script `run_all_fixed.sh` to start all necessary services including Celery workers, beat scheduler, and the RAG API app.

Make sure to give execute permission to the script:
```
chmod +x run_all_fixed.sh
```

Then run:
```
./run_all_fixed.sh



Swagger-ui (rag test)

http://localhost:8000/docs 

please find the curl commands in the swagger-ui to test the rag pipeline and also you can use the swagger-ui to test the rag pipeline.
