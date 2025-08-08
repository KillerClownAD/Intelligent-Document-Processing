# clear_state.py (Updated for Collection-per-User architecture)
import argparse
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration (Must match your mongodb_state_db.py) ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "rag_user_metadata" # Use the new DB name

def main():
    """A tool to inspect and clear all user collections in the database."""
    parser = argparse.ArgumentParser(description="Manage user-specific RAG pipeline states in MongoDB.")
    parser.add_argument(
        "--action",
        choices=['list', 'delete'],
        default='list',
        help="Choose 'list' to see current states or 'delete' to clear all user collections. Defaults to 'list'."
    )
    args = parser.parse_args()

    try:
        print(f"Connecting to MongoDB at {MONGO_URI}...")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        print(f"✅ Connection successful. Using database '{DB_NAME}'.")
    except Exception as e:
        print(f"❌ Could not connect to MongoDB: {e}")
        return

    # Get all collection names (which correspond to user_ids)
    user_collections = db.list_collection_names()

    if not user_collections:
        print("No user collections found in the database.")
        return

    print(f"Found {len(user_collections)} user collections: {user_collections}\n")

    if args.action == 'list':
        for user_id in user_collections:
            print(f"--- 📝 Current States for user '{user_id}' ---")
            collection = db[user_id]
            states = list(collection.find({}))
            if not states:
                print(" (empty)")
            else:
                for state in states:
                    print(state)
            print("-" * 40)

    elif args.action == 'delete':
        print(f"--- 🔥 Deleting ALL data from ALL user collections in '{DB_NAME}' ---")
        if input("Are you sure you want to proceed? This will drop all user collections. (y/n): ").lower() != 'y':
            print("Aborted.")
            return

        for user_id in user_collections:
            db.drop_collection(user_id)
            print(f"  - Dropped collection for user '{user_id}'.")

        print("\n✅ Successfully cleared all user states.")
        print("The pipeline will now re-process all files on the next scan.")

if __name__ == "__main__":
    main()