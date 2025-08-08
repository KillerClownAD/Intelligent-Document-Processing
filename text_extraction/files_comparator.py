# files_comparator.py
from typing import List
from dataclasses import dataclass

@dataclass
class FileMetadata:
    user_id: str
    file_path: str
    folder_path: str
    file_name: str
    size_bytes: int
    extension: str
    last_modified: str
    sha256: str

class SyncAction:
    ADD = "add"
    UPDATE = "modified"
    DELETE = "deleted"
    NO_CHANGE = "no_change"

@dataclass
class SyncResult:
    action: str
    file_metadata: FileMetadata
    reason: str

class FileSyncComparator:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose  # Optional flag for debug logging

    def _log(self, message: str):
        if self.verbose:
            print(f"[FileSyncComparator] {message}")

    def _needs_update(self, fs_meta: FileMetadata, db_meta: FileMetadata) -> bool:
        """Compares two FileMetadata objects to see if an update is needed."""
        # Comparing sha256 hash and last_modified timestamp
        return (
            fs_meta.sha256 != db_meta.sha256 or
            fs_meta.last_modified != db_meta.last_modified
        )

    def compare_user_files(self, db_files: List[FileMetadata], fs_files: List[FileMetadata]) -> List[SyncResult]:
        """Compares file metadata for a single user."""
        db_map = {file.file_path: file for file in db_files}
        fs_map = {file.file_path: file for file in fs_files}

        sync_results: List[SyncResult] = []

        # Check for new or updated files
        for file_path, fs_meta in fs_map.items():
            if file_path not in db_map:
                self._log(f"ADD: {file_path}")
                sync_results.append(SyncResult(
                    action=SyncAction.ADD, file_metadata=fs_meta,
                    reason="File not found in database"
                ))
            else:
                db_meta = db_map[file_path]
                if self._needs_update(fs_meta, db_meta):
                    self._log(f"UPDATE: {file_path}")
                    sync_results.append(SyncResult(
                        action=SyncAction.UPDATE, file_metadata=fs_meta,
                        reason="File content or metadata changed"
                    ))
                else:
                    pass

        # Check for deleted files
        for file_path, db_meta in db_map.items():
            if file_path not in fs_map:
                self._log(f"DELETE: {file_path}")
                sync_results.append(SyncResult(
                    action=SyncAction.DELETE, file_metadata=db_meta,
                    reason="File no longer exists in filesystem"
                ))

        return sync_results
