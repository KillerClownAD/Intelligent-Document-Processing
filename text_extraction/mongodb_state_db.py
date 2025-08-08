# mongodb_state_db.py

from pymongo import MongoClient, UpdateOne
import os
from typing import List, Optional, Dict, Any
import uuid

from text_extraction.files_comparator import FileMetadata, SyncResult, SyncAction

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "test_metadata"

_client = None
_db = None

def get_db():
    """Establishes a fork-safe connection to the database."""
    global _client, _db
    if _db is None:
        try:
            _client = MongoClient(MONGO_URI)
            _db = _client[DB_NAME]
        except Exception as e:
            print(f"❌ Could not connect to MongoDB: {e}")
            raise
    return _db

def get_file_document(user_id: str, file_path: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves the complete document for a single file from MongoDB.
    """
    db = get_db()
    collection = db[user_id]
    return collection.find_one({"file_path": file_path})

def get_user_file_states(user_id: str) -> List[FileMetadata]:
    """
    Retrieves all file metadata for a specific user to represent the last known state.
    """
    db = get_db()
    collection = db[user_id]
    states = []
    # Find documents that were not marked as 'deleted'
    for doc in collection.find({"status": {"$ne": "deleted"}}):
        if doc and doc.get("file_path") and doc.get("file_name"):
            states.append(FileMetadata(
                user_id=doc.get("user_id", user_id),
                file_path=doc.get("file_path"),
                folder_path=doc.get("folder_path"),
                file_name=doc.get("file_name"),
                size_bytes=doc.get("size_bytes"),
                extension=doc.get("extension"),
                last_modified=doc.get("last_modified"),
                sha256=doc.get("sha256"),
            ))
    return states

def apply_sync_results(user_id: str, results: List[SyncResult]):
    """
    Applies the results of a file comparison to the database.
    This function handles adding, updating, and marking files as deleted.
    """
    db = get_db()
    collection = db[user_id]
    
    bulk_operations = []

    for result in results:
        meta = result.file_metadata
        
        # Prepare document based on metadata
        doc = {
            "user_id": meta.user_id,
            "file_path": meta.file_path,
            "folder_path": meta.folder_path,
            "file_name": meta.file_name,
            "size_bytes": meta.size_bytes,
            "extension": meta.extension,
            "last_modified": meta.last_modified,
            "sha256": meta.sha256,
            "status": result.action,
        }

        # For new files, we add a UUID
        if result.action == SyncAction.ADD:
            doc["uuid"] = str(uuid.uuid4())

        # Use file_path as the unique identifier for upserting
        filter_query = {"file_path": meta.file_path}
        
        # For deleted action, we just update the status
        if result.action == SyncAction.DELETE:
            operation = UpdateOne(filter_query, {"$set": {"status": result.action}})
        else: # For add/update, we upsert the entire document
            operation = UpdateOne(filter_query, {"$set": doc}, upsert=True)
            
        bulk_operations.append(operation)

    if bulk_operations:
        collection.bulk_write(bulk_operations)
        print(f"Applied {len(bulk_operations)} state changes to MongoDB for user '{user_id}'.")