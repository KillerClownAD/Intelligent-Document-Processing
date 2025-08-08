from worker import process_file

payload_shilpa = {
    "uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567895",
    "sha256": "f4e2b8a2e5d9f1c8d7b6a5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e5",
    "user_id": "darshan",
    "file_name": "b.pdf",
    "file_path": "/share/iv-data/darshan/files/sub/b.pdf",
    "folder_path": "/share/iv-data/darshan/files/sub",
    "status": "add",  # Treat "COMPLETED" as "add" for indexing purposes
    "last_updated": "2025-07-15T12:30:00Z",
    "extracted_text": {
        "page_1": "darshan is from chennai.",
        "page_2": "stays in near by mk circle",
        "page_3": "works as a devops engineer."
    }
}

process_file.delay(payload_shilpa)



