# idp_app/tasks.py

import hashlib
from pathlib import Path
from datetime import datetime, timezone

from celery import shared_task
from text_extraction.mongodb_state_db import apply_sync_results
from text_extraction.files_comparator import FileMetadata, SyncAction, SyncResult
from text_extraction.tasks import docvlm_extraction_task

def get_file_sha256(filepath: Path) -> str:
    """Calculates the SHA-256 hash of a file's content."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

@shared_task(name="idp_app.tasks.process_file")
def process_file(filepaths: list):
    """
    Receives a list of file paths from an external app, logs them in the database,
    and queues them for document extraction.
    """
    print(f"Received file processing request from IDP app for: {filepaths}")
    
    for file_path_str in filepaths:
        try:
            file_path = Path(file_path_str)
            if not file_path.exists():
                print(f"⚠️ File not found, skipping: {file_path_str}")
                continue

            # Assuming the user_id is the second to last part of the path
            # e.g., /path/to/data/<user_id>/files/<filename>
            user_id = file_path.parent.parent.name
            stats = file_path.stat()

            # Create a FileMetadata object, just like the scanner does
            file_meta = FileMetadata(
                user_id=user_id,
                file_path=str(file_path),
                folder_path=str(file_path.parent),
                file_name=file_path.name,
                size_bytes=stats.st_size,
                extension=file_path.suffix,
                last_modified=datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc).isoformat(),
                sha256=get_file_sha256(file_path),
            )

            # Create a SyncResult to add this file to the database
            sync_result = SyncResult(
                action=SyncAction.ADD,
                file_metadata=file_meta,
                reason="File added via IDP application"
            )
            
            # 1. Apply the change to the MongoDB state database
            apply_sync_results(user_id, [sync_result])

            # 2. Queue the document for extraction
            docvlm_extraction_task.delay(user_id, file_meta.file_path, file_meta.sha256)
            
            print(f"✅ Successfully queued '{file_meta.file_name}' for user '{user_id}'.")

        except Exception as e:
            print(f"❌ Error processing file {file_path_str} from IDP app: {e}")