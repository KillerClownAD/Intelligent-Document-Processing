from rag_query_pipeline import rag_pipeline

response = rag_pipeline(
    user_id="darshan",
    session_id="sess-1",
    user_query="who is the judge for case 2009 SCC OnLine ITAT 1081 : [2009] ITAT 442",
    type="all",  # options: "file", "folder", "all"
    file_or_folder_path=""
)

if isinstance(response, dict):
    print("\nAnswer:\n", response["answer"])
else:
    print("\n[ERROR] Pipeline failed:\n", response)