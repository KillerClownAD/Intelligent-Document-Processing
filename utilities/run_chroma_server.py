# run_chroma_server.py (Corrected Version)
# This script manually starts the ChromaDB CLI application,
# bypassing the missing command-line tool.

import sys
try:
    # This is the entry point for modern versions of ChromaDB's CLI
    from chromadb.cli.cli import app
except ImportError:
    print("Error: Could not import the ChromaDB CLI application.")
    print("Please ensure 'chromadb' is correctly installed in your virtual environment.")
    sys.exit(1)

if __name__ == "__main__":
    # This is equivalent to running 'chromadb' from the command line.
    # It will automatically process arguments passed to the script,
    # such as 'run', '--path', etc.
    sys.exit(app())
