# pipeline_logic.py
import uuid
import hashlib
from datetime import datetime, timezone
import os

from pymongo import MongoClient
import chromadb

# --- MODIFIED LINE ---
# Update this URI to match the one in mongodb_state_db.py
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "rag_pipeline_db"
METADATA_COLLECTION = "document_metadata"
PROCESSED_JSON_COLLECTION = "processed_json_files"

# --- Database Clients (Initialized once per process) ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
metadata_col = db[METADATA_COLLECTION]
processed_json_col = db[PROCESSED_JSON_COLLECTION]

# This assumes your vector DB is local and doesn't need the auth
chroma_client = chromadb.PersistentClient(path="./rag_local_db")
vector_collection = chroma_client.get_or_create_collection("document_vectors")


# === 1. UUID & Hashing Functions ===

def generate_document_id():
    """Generates a new unique ID for a document."""
    return str(uuid.uuid4())

def compute_file_hash(filepath):
    """Calculates the SHA-256 hash of a file's content."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


# === 2. State Management Functions (for JSON files) ===

def check_if_json_is_processed(json_content_hash):
    """Checks MongoDB to see if a JSON file with this exact content has already been processed."""
    return processed_json_col.find_one({"_id": json_content_hash}) is not None

def log_processed_json(json_filename, json_content_hash):
    """Logs a record that a JSON file has been successfully processed."""
    processed_json_col.update_one(
        {"_id": json_content_hash},
        {"$set": {"filename": json_filename, "processed_time": datetime.now(timezone.utc)}},
        upsert=True
    )


# === 3. Data Ingestion Functions ===

def add_document_metadata_to_mongo(doc_id, user_id, doc_filename, chunk_count):
    """Adds or updates the metadata for a single document in MongoDB."""
    metadata_col.update_one(
        {"_id": doc_id},
        {"$set": {
            "user_id": user_id,
            "filename": doc_filename,
            "chunk_count": chunk_count,
            "ingested_time": datetime.now(timezone.utc)
        }},
        upsert=True
    )

def insert_embeddings_to_chroma(doc_id, embeddings, chunks):
    """Inserts document chunks and their embeddings into ChromaDB with linked metadata."""
    ids = []
    metadatas = []
    
    for i, chunk_text in enumerate(chunks):
        chunk_id = f"{doc_id}_{i}"
        ids.append(chunk_id)
        metadatas.append({
            "document_id": doc_id, # Link back to the document metadata in MongoDB
            "chunk_index": i
        })
        
    vector_collection.upsert(
        ids=ids,
        embeddings=embeddings,
        documents=chunks,
        metadatas=metadatas
    )


# === 4. Deletion Functions ===

def delete_document_from_all_dbs(user_id, doc_filename):
    """Safely deletes a document's metadata from Mongo and all its vectors from Chroma."""
    # This function will now use the authenticated client
    print(f"🔥 Deleting document '{doc_filename}' for user '{user_id}'...")
    
    # Find the document in MongoDB to get its ID
    doc_record = metadata_col.find_one({"user_id": user_id, "filename": doc_filename})
    
    if not doc_record:
        print(f"  -> Document not found in MongoDB. Nothing to delete.")
        return

    doc_id = doc_record["_id"]

    # Delete vectors from ChromaDB using the document_id
    vector_collection.delete(where={"document_id": doc_id})
    print(f"  -> Deleted vectors from ChromaDB.")

    # Delete metadata record from MongoDB
    metadata_col.delete_one({"_id": doc_id})
    print(f"  -> Deleted metadata from MongoDB.")