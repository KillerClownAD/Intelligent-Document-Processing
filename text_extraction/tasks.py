# tasks.py

import os
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from celery import shared_task
from text_extraction.mongodb_state_db import get_user_file_states, apply_sync_results
from text_extraction.docvlm_task import docvlm_extraction_task
from text_extraction.files_comparator import FileMetadata, FileSyncComparator, SyncResult, SyncAction
from text_extraction.pipeline_logic import delete_document_from_all_dbs


SOURCE_DATA_PATH = Path(__file__).parent / "source_documents"

def get_file_sha256(filepath: Path) -> str:
    """Calculates the SHA-256 hash of a file's content."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def _scan_user_disk_files(user_id: str, user_path: Path) -> List[FileMetadata]:
    """Scans the filesystem for a user and returns a list of FileMetadata."""
    disk_files = []
    if not user_path.is_dir():
        return []
        
    for file_path in user_path.iterdir():
        if file_path.is_file():
            stats = file_path.stat()
            disk_files.append(FileMetadata(
                user_id=user_id,
                file_path=str(file_path),
                folder_path=str(file_path.parent),
                file_name=file_path.name,
                size_bytes=stats.st_size,
                extension=file_path.suffix,
                last_modified=datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc).isoformat(),
                sha256=get_file_sha256(file_path),
            ))
    return disk_files


@shared_task(name="tasks.discover_users_and_dispatch_task")
def discover_users_and_dispatch_task():
    """
    The main scheduler task. Finds all user directories and launches a
    separate, independent scanner task for each one.
    """
    print(f"---  M-Task: Discovering user directories in {SOURCE_DATA_PATH} ---")
    if not SOURCE_DATA_PATH.is_dir():
        print(f"⚠️ Master task failed: Source data path not found: {SOURCE_DATA_PATH}")
        return

    for user_dir in SOURCE_DATA_PATH.iterdir():
        if user_dir.is_dir():
            user_id = user_dir.name
            print(f" M-Task: Found user '{user_id}'. Dispatching scanner task.")
            scan_user_files_task.delay(user_id)

@shared_task(name="tasks.scan_user_files_task")
def scan_user_files_task(user_id: str):
    """
    Scans a user's directory, compares state with MongoDB, updates the database
    with the changes, and queues tasks for new, updated, or deleted files.
    """
    print(f"🔍 U-Task ({user_id}): Scanning files...")
    user_files_path = SOURCE_DATA_PATH / user_id / "files"

    # 1. Get current state from the filesystem and database
    fs_files = _scan_user_disk_files(user_id, user_files_path)
    db_files = get_user_file_states(user_id)

    # 2. Compare the two states to get a list of actions
    comparator = FileSyncComparator(verbose=True)
    sync_results = comparator.compare_user_files(db_files=db_files, fs_files=fs_files)

    if not sync_results:
        print(f" U-Task ({user_id}): No changes detected.")
        return

    # 3. Apply all state changes to the MongoDB database
    apply_sync_results(user_id, sync_results)

    # 4. Queue tasks based on the actions
    for result in sync_results:
        meta = result.file_metadata
        if result.action in [SyncAction.ADD, SyncAction.UPDATE]:
            print(f" U-Task ({user_id}): Queuing '{meta.file_name}' for processing (Reason: {result.action}).")
            docvlm_extraction_task.delay(user_id, meta.file_path, meta.sha256)
            
        elif result.action == SyncAction.DELETE:
            print(f" U-Task ({user_id}): Queuing '{meta.file_name}' for deletion from vector DB.")
            from data_ingestion.worker import process_file
            payload = {
                "user_id": user_id,
                "file_name": meta.file_name,
                "file_path": meta.file_path,
                "folder_path": meta.folder_path,
                "extension": meta.extension,
                "last_modified": meta.last_modified,
                "sha256": meta.sha256,
                "size_bytes": meta.size_bytes,
                "status": "deleted",
                "uuid": getattr(meta, "uuid", None) or "",  # Use empty string if uuid not present
            }
            process_file.delay(payload)
