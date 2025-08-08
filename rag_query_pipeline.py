import requests
import os
from dotenv import load_dotenv
from chromadb import HttpClient  # ✅ UPDATED
from pymongo import MongoClient
from datetime import datetime

# ---- Load Environment Variables ----
load_dotenv()

# ---- Environment Variables ----
TEXT_URL = os.getenv("TEXT_URL")
EMBED_URL = os.getenv("EMBED_URL")
EMBED_MODEL = os.getenv("EMBED_MODEL")
RERANK_URL = os.getenv("RERANK_URL")
RERANK_MODEL = os.getenv("RERANK_MODEL")
CHROMA_HOST = os.getenv("CHROMA_HOST")  # ✅ UPDATED

# ---- MongoDB ----
MONGO_URI = os.getenv("MONGO_URI")

# ---- MongoDB Setup ----
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["rag_db"]
chat_collection = mongo_db["chat_history"]

# ---- ChromaDB (Remote) ----
chroma_client = HttpClient(host=CHROMA_HOST)  # ✅ UPDATED

# ---- Embedding ----
def get_embedding(query):
    try:
        res = requests.post(EMBED_URL, json={
            "input": [query],
            "model": EMBED_MODEL,
            "input_type": "query"
        }, headers={"Content-Type": "application/json"})
        return res.json()["data"][0]["embedding"]
    except Exception as e:
        print(f"[Embedding Error] {e}")
        return []

# ---- Reranking ----
def rerank_chunks(query, passages):
    try:
        payload = {
            "model": RERANK_MODEL,
            "query": {"text": query},
            "passages": [{"text": p} for p in passages],
            "truncate": "END"
        }
        res = requests.post(RERANK_URL, json=payload, headers={"Content-Type": "application/json"})
        data = res.json()

        if "rankings" not in data:
            print(f"[Rerank API Response Missing 'rankings']: {data}")
            return [(p, None) for p in passages]  # fallback: no scores

        ranks = data["rankings"]
        ranked_passages = sorted(ranks, key=lambda x: -x["logit"])
        return [(passages[r["index"]], r["logit"]) for r in ranked_passages]

    except Exception as e:
        print(f"[Rerank Error] {e}")
        return [(p, None) for p in passages]

# ---- Get Session History ----
def get_session_history(user_id, session_id):
    user_doc = chat_collection.find_one({"user_id": user_id})
    if not user_doc:
        return []
    
    session = next((s for s in user_doc.get("sessions", []) if s["session_id"] == session_id), None)
    return session.get("history", []) if session else []

# ---- Objective Generation ----
def generate_objective_from_query(user_query):
    try:
        prompt = f"You are generating a session title (3-6 words) based on the user query and the retrieved document content. Do NOT guess or hallucinate names, professions, or entities. Only use information explicitly present in the retrieved content. If the full name is not mentioned, use a neutral placeholder like Profile Summary. And only respond with title.: '{user_query}'"
        res = requests.post(TEXT_URL, json={
            "model": "meta/llama-3.1-70b-instruct",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 20
        }, headers={"Content-Type": "application/json"})
        return res.json()["choices"][0]["message"]["content"].strip().strip('"')
    except Exception as e:
        print(f"[Objective Generation Error] {e}")
        return user_query[:60] + "..."

# ---- Update Chat History ----
def update_session_history(user_id, session_id, user_query, assistant_answer):
    user_doc = chat_collection.find_one({"user_id": user_id})
    
    if not user_doc:
        objective = generate_objective_from_query(user_query)
        chat_collection.insert_one({
            "user_id": user_id,
            "sessions": [{
                "session_id": session_id,
                "objective": objective,
                "created_at": datetime.utcnow(),
                "history": [{"query": user_query, "answer": assistant_answer}]
            }]
        })
    else:
        sessions = user_doc.get("sessions", [])
        session_index = next((i for i, s in enumerate(sessions) if s["session_id"] == session_id), None)

        if session_index is None:
            objective = generate_objective_from_query(user_query)
            chat_collection.update_one(
                {"user_id": user_id},
                {"$push": {
                    "sessions": {
                        "session_id": session_id,
                        "objective": objective,
                        "created_at": datetime.utcnow(),
                        "history": [{"query": user_query, "answer": assistant_answer}]
                    }
                }}
            )
        else:
            history_path = f"sessions.{session_index}.history"
            chat_collection.update_one(
                {"user_id": user_id},
                {"$push": {history_path: {"query": user_query, "answer": assistant_answer}}}
            )

# ---- Call LLM ----
def call_llm_rag(user_query, context_chunks, history):
    joined_context = "\n---\n".join(context_chunks)

    prior_msgs = []
    for item in history:
        prior_msgs.append({"role": "user", "content": item["query"]})
        prior_msgs.append({"role": "assistant", "content": item["answer"]})

    full_messages = prior_msgs + [
        {"role": "system", "content": "You are a QA assistant. Please answer only from the given context. If the query is not related to the context, please reply with 'I don't know the answer.'"},
        {"role": "user", "content": f"Context:\n{joined_context}\n\nQuestion: {user_query}\nAnswer:"}
    ]

    try:
        res = requests.post(TEXT_URL, json={
            "model": "meta/llama-3.1-70b-instruct",
            "messages": full_messages,
            "max_tokens": 1024
        }, headers={"Content-Type": "application/json"})
        return res.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[LLM Error] {e}")
        return "Sorry, the assistant couldn't answer due to an internal error."

# ---- Main RAG Pipeline ----
def rag_pipeline(
    user_id: str,
    session_id: str,
    user_query: str,
    type: str,
    file_or_folder_path: str = "",
    top_k: int = 5
):
    print(f"\n[INFO] RAG for user='{user_id}', session='{session_id}', query='{user_query}'")

    # Step 1: Embed the query
    query_embedding = get_embedding(user_query)
    if not query_embedding:
        return "Embedding failed. Cannot process your request."

    # Step 2: Retrieve ChromaDB collection
    try:
        collection = chroma_client.get_collection(name=f"{user_id}_chunks")
    except Exception as e:
        return f"[ERROR] No chunk collection found for user {user_id}: {e}"

    # Step 3: Query chunks
    try:
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=100,
            include=["documents", "metadatas", "distances"]
        )
    except Exception as e:
        return f"[ERROR] Querying Chroma failed: {e}"

    # Step 4: Filter chunks
    docs = []

    if type == "all":
        docs = results["documents"][0]
    else:
        is_folder = (type == "folder")
        for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
            path = meta.get("file_path", "")
            if (is_folder and path.startswith(file_or_folder_path)) or (not is_folder and path == file_or_folder_path):
                docs.append(doc)

        if not docs:
            return f"No relevant chunks found for '{file_or_folder_path}'."

    # Step 5: Rerank filtered chunks
    reranked_docs_with_scores = rerank_chunks(user_query, docs)

    #  Print reranked chunks with logit scores
    print("\n[DEBUG] Reranked chunks sent to LLM:")
    for i, (chunk, score) in enumerate(reranked_docs_with_scores[:top_k]):
        print(f"\n[Chunk {i+1}] (logit: {score}):\n{chunk}\n---")

    # Extract only the chunk texts to send to LLM
    reranked_docs = [doc for doc, _ in reranked_docs_with_scores[:top_k]]

    # Step 6: Get prior history
    prior_history = get_session_history(user_id, session_id)

    # Step 7: Call LLM
    answer = call_llm_rag(user_query, reranked_docs, prior_history)

    # Step 8: Store full query-answer pair in Mongo
    update_session_history(user_id, session_id, user_query, answer)

    return {
        "session_id": session_id,
        "query": user_query,
        "type": type,
        "file_or_folder": file_or_folder_path,
        "chunks_used": reranked_docs,
        "answer": answer
    }
