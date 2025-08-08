import os
from dotenv import load_dotenv

load_dotenv()

import requests
from datetime import datetime
from pymongo import MongoClient
from langchain.text_splitter import RecursiveCharacterTextSplitter
from chromadb import HttpClient  # CHANGED
from celery import Celery
import tiktoken

MONGO_URI = os.getenv("MONGO_URI")
CHROMA_HOST = os.getenv("CHROMA_HOST")  # CHANGED
TEXT_URL = os.getenv("TEXT_URL")
EMBED_URL = os.getenv("EMBED_URL")
EMBED_MODEL = os.getenv("EMBED_MODEL")
SUMMARY_TOKEN_LIMIT = int(os.getenv("SUMMARY_TOKEN_LIMIT"))

app = Celery('data_ingestion_app', broker=os.getenv('BROKER_URL'), backend=os.getenv('BACKEND_URL'), include=['data_ingestion.worker'])

app.conf.task_default_queue = 'data_ingestion_queue'
app.conf.task_queues = {
    'data_ingestion_queue': {
        'exchange': 'data_ingestion',
        'routing_key': 'data_ingestion',
    },
}
app.conf.task_routes = {
    'data_ingestion.*': {'queue': 'data_ingestion_queue'},
}


def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client['summary_db']['collection_of_summaries']


def call_llm(prompt):
    try:
        res = requests.post(TEXT_URL, json={
            "model": "meta/llama-3.1-70b-instruct",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 2048
        }, headers={"Content-Type": "application/json"})
        return res.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[ERROR] LLM call failed: {e}")
        return "Summary generation failed."


def get_embedding(text, input_type="passage"):
    try:
        res = requests.post(EMBED_URL, json={
            "input": [text], "model": EMBED_MODEL, "input_type": input_type
        }, headers={"Content-Type": "application/json"})
        return res.json()["data"][0]["embedding"]
    except Exception as e:
        print(f"[ERROR] Embedding call failed: {e}")
        return []


def chunk_text(text):
    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
    return splitter.split_text(text)


def count_tokens(text, model="llama"):
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def summarize_text(text):
    token_count = count_tokens(text)
    if token_count <= SUMMARY_TOKEN_LIMIT:
        print(f"[INFO] Text within {SUMMARY_TOKEN_LIMIT} tokens. Sending full text to LLM.")
        return call_llm(f"Summarize this document:\n\n{text}")
    else:
        print(f"[INFO] Text exceeds {SUMMARY_TOKEN_LIMIT} tokens. Summarizing chunks...")
        chunks = chunk_text(text)
        chunk_summaries = []
        for i, chunk in enumerate(chunks):
            chunk_summary = call_llm(f"Summarize this section ({i+1}):\n\n{chunk}")
            chunk_summaries.append(chunk_summary)
        merged_summary_prompt = "Merge these summaries into one cohesive summary:\n\n" + "\n\n".join(chunk_summaries)
        return call_llm(merged_summary_prompt)


@app.task(bind=True)
def process_file(self, payload):
    try:
        user_id = payload['user_id']
        file_name = payload['file_name']
        file_uuid = payload['uuid']
        sha256 = payload['sha256']
        file_path = payload.get('file_path', '')
        folder_path = payload.get('folder_path', '')
        status = payload['status'].lower()
        last_updated = payload.get("last_updated", datetime.utcnow().isoformat())
        extracted_text = payload.get('extracted_text', {})
        text = "\n".join(extracted_text.values())

        collection_name = f"{user_id}_chunks"
        summary_collection_name = f"{user_id}_summaries"

        mongo_collection = get_mongo_collection()

        # CHANGED: use HttpClient instead of PersistentClient
        chroma_client = HttpClient(host=CHROMA_HOST)

        collection = chroma_client.get_or_create_collection(name=collection_name)
        summary_collection = chroma_client.get_or_create_collection(name=summary_collection_name)

        if status == 'deleted':
            collection.delete(where={"filename": file_name})
            summary_collection.delete(where={"filename": file_name})
            mongo_collection.update_one(
                {"user_id": user_id},
                {"$pull": {"files": {"filename": file_name}}}
            )
            print(f"[INFO] Deleted all vectors and metadata for {file_name}")
            return

        if status == 'modified':
            collection.delete(where={"filename": file_name})
            summary_collection.delete(where={"filename": file_name})
            mongo_collection.update_one(
                {"user_id": user_id},
                {"$pull": {"files": {"filename": file_name}}}
            )
            print(f"[INFO] Cleared old data for modified file {file_name}")

        if status in ('add', 'modified'):
            chunks = chunk_text(text)
            print(f"[INFO] Chunking complete: {len(chunks)} chunks")

            ids, embeddings, metadatas = [], [], []
            for idx, chunk in enumerate(chunks):
                emb = get_embedding(chunk)
                if not emb:
                    continue
                ids.append(f"{file_uuid}_{idx}")
                embeddings.append(emb)
                metadatas.append({
                    "user_id": user_id,
                    "filename": file_name,
                    "file_path": file_path,
                    "folder_path": folder_path,
                    "uuid": file_uuid,
                    "sha256": sha256,
                    "chunk_index": idx,
                    "timestamp": datetime.utcnow().isoformat()
                })

            if embeddings:
                collection.add(ids=ids, embeddings=embeddings, documents=chunks, metadatas=metadatas)
                print(f"[INFO] Stored {len(embeddings)} embeddings for {file_name}")
            else:
                print(f"[WARN] No valid embeddings generated for {file_name}")

            # Generate and store summary using token-limit-aware approach
            summary = summarize_text(text)

            user_doc = mongo_collection.find_one({"user_id": user_id})

            summary_metadata = {
                "uuid": file_uuid,
                "filename": file_name,
                "file_path": file_path,
                "folder_path": folder_path,
                "sha256": sha256,
                "status": status,
                "summary": summary,
                "last_updated": last_updated
            }

            if user_doc:
                existing_files = user_doc.get("files", [])
                file_exists = any(f["filename"] == file_name for f in existing_files)
                if file_exists:
                    mongo_collection.update_one(
                        {"user_id": user_id, "files.filename": file_name},
                        {"$set": {"files.$": summary_metadata}}
                    )
                    print(f"[INFO] Updated existing summary for {file_name} under user {user_id}")
                else:
                    mongo_collection.update_one(
                        {"user_id": user_id},
                        {"$push": {"files": summary_metadata}}
                    )
                    print(f"[INFO] Added new summary for {file_name} under user {user_id}")
            else:
                new_user_doc = {
                    "user_id": user_id,
                    "files": [summary_metadata]
                }
                mongo_collection.insert_one(new_user_doc)
                print(f"[INFO] Created new user entry for {user_id} with file {file_name}")

            # Store in Chroma summary collection
            chroma_summary_metadata = {
                k: str(v) if isinstance(v, datetime) else v
                for k, v in summary_metadata.items()
            }

            summary_collection.add(
                documents=[summary],
                metadatas=[chroma_summary_metadata],
                ids=[f"summary_{file_uuid}"]
            )
            print(f"[INFO] Summary stored in Chroma for {file_name}")

    except Exception as e:
        print(f"[FATAL ERROR] Task failed: {e}")
        self.retry(exc=e, countdown=10, max_retries=3)
