# check_paths.py
import os
from pathlib import Path

# This path must match the one in tasks.py
SOURCE_DATA_PATH = Path("/share/iv-data")

print(f"--- 🕵️  Running Path and Permission Check ---")
print(f"Checking base path: {SOURCE_DATA_PATH}\n")

# 1. Check if the base path exists
if not SOURCE_DATA_PATH.exists():
    print(f"❌ ERROR: The base path does not exist!")
    exit()
elif not SOURCE_DATA_PATH.is_dir():
    print(f"❌ ERROR: The base path is not a directory!")
    exit()
else:
    print(f"✅ SUCCESS: Base path exists and is a directory.")

# 2. Check for read/execute permissions
can_read = os.access(SOURCE_DATA_PATH, os.R_OK)
can_execute = os.access(SOURCE_DATA_PATH, os.X_OK) # Execute is needed to list a directory's contents

if not (can_read and can_execute):
    print(f"❌ ERROR: Insufficient permissions. Cannot read or list contents of the directory.")
    print(f"   - Can Read: {can_read}")
    print(f"   - Can List/Execute: {can_execute}")
    exit()
else:
    print(f"✅ SUCCESS: Permissions are sufficient to read and list contents.\n")


# 3. Check the directory structure and content
print("--- 📂 Checking Directory Contents ---")
user_dirs_found = 0
files_found = 0

try:
    for user_dir in SOURCE_DATA_PATH.iterdir():
        if not user_dir.is_dir():
            print(f"  - Skipping non-directory: {user_dir.name}")
            continue
        
        user_dirs_found += 1
        print(f"\n  ▶️ Found user directory: {user_dir.name}")
        
        files_dir = user_dir / "files"
        if not files_dir.is_dir():
            print(f"    - ⚠️  WARNING: No 'files' subdirectory found for this user.")
            continue
            
        print(f"    - ✅ Found 'files' subdirectory.")
        
        user_files = list(files_dir.iterdir())
        if not user_files:
            print(f"    - ⚠️  WARNING: The 'files' subdirectory is empty.")
        else:
            for file_path in user_files:
                if file_path.is_file():
                    files_found += 1
                    print(f"      - 📄 Found file: {file_path.name}")
except Exception as e:
    print(f"\n❌ ERROR while scanning directories: {e}")


print("\n--- 📊 Summary ---")
print(f"Total user directories found: {user_dirs_found}")
print(f"Total files found: {files_found}")

if user_dirs_found == 0 or files_found == 0:
    print("\n‼️ ACTION: The scanner is not finding any files. Check the output above to see if the path is wrong, permissions are denied, or the directory structure is incorrect.")
