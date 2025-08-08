import os
from dotenv import load_dotenv
from chromadb import HttpClient

# Load CHROMA_HOST from .env
load_dotenv()
CHROMA_HOST = os.getenv("CHROMA_HOST")

# Connect to ChromaDB
chroma_client = HttpClient(host=CHROMA_HOST)

# List all collections
def list_collections():
    collections = chroma_client.list_collections()
    print("\n[📁 Collections in ChromaDB]")
    for col in collections:
        print(f"- {col.name}")
    return [col.name for col in collections]

# Show sample data from a collection
def inspect_collection(collection_name, sample_size=5):
    try:
        collection = chroma_client.get_collection(name=collection_name)
        print(f"\n[🔍 Inspecting Collection: {collection_name}]")
        results = collection.query(
            query_texts=["example"],
            n_results=sample_size,
            include=["documents", "metadatas"]
        )

        for i, (doc, meta) in enumerate(zip(results["documents"][0], results["metadatas"][0])):
            print(f"\n--- Document #{i+1} ---")
            print("Document:\n", doc[:300], "...")  # Truncate for readability
            print("Metadata:", meta)

    except Exception as e:
        print(f"[ERROR] Failed to inspect collection '{collection_name}': {e}")

# Run it
if __name__ == "__main__":
    all_collections = list_collections()
    
    # Optional: auto-inspect a specific collection
    if all_collections:
        inspect_collection(all_collections[0])
