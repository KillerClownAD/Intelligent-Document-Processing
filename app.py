from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime
from rag_query_pipeline import rag_pipeline
from pymongo import MongoClient
from dotenv import load_dotenv
from uuid import uuid4
import os
import uvicorn

# ---- Load Environment Variables ----
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# ---- MongoDB Setup ----
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["rag_db"]
chat_collection = mongo_db["chat_history"]

# ---- FastAPI App ----
app = FastAPI(title="RAG API", version="1.0")

# ---- Request Schema for RAG ----
class RAGRequest(BaseModel):
    user_id: str
    session_id: str
    user_query: str
    type: str  # "file", "folder", or "all"
    file_or_folder_path: Optional[str] = ""

# ---- Chat Message Schema ----
class ChatMessage(BaseModel):
    query: str
    answer: str

# ---- Response for RAG ----
class RAGResponse(BaseModel):
    session_id: str
    query: str
    type: str
    file_or_folder: str
    chunks_used: List[str]
    answer: str

# ---- Session History Schema ----
class HistoryResponse(BaseModel):
    session_id: str
    objective: str
    created_at: datetime
    history: List[ChatMessage]

# ---- Full User History Schema ----
class SessionHistory(BaseModel):
    session_id: str
    objective: str
    created_at: datetime
    history: List[ChatMessage]

# ---- RAG Endpoint ----
@app.post("/rag", response_model=Union[RAGResponse, dict])
def handle_rag_query(payload: RAGRequest):
    try:
        #  Generate new session ID if not provided or empty
        session_id = payload.session_id.strip() or str(uuid4())

        response = rag_pipeline(
            user_id=payload.user_id,
            session_id=session_id,
            user_query=payload.user_query,
            type=payload.type,
            file_or_folder_path=payload.file_or_folder_path or ""
        )

        if isinstance(response, dict):
            #  Ensure updated session_id is returned in the response
            response["session_id"] = session_id
            return response
        else:
            raise HTTPException(status_code=500, detail=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---- Retrieve Specific Session History ----
@app.get("/session_history", response_model=HistoryResponse)
def get_chat_history(user_id: str = Query(...), session_id: str = Query(...)):
    user_doc = chat_collection.find_one({"user_id": user_id})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found.")

    sessions = user_doc.get("sessions", [])
    session = next((s for s in sessions if s["session_id"] == session_id), None)

    if not session:
        raise HTTPException(status_code=404, detail="Session not found.")

    return {
        "session_id": session["session_id"],
        "objective": session.get("objective", ""),
        "created_at": session.get("created_at", datetime.utcnow()),
        "history": session.get("history", [])
    }


# ---- Retrieve All Sessions for a User ----
@app.get("/full_history", response_model=List[SessionHistory])
def get_full_user_history(user_id: str = Query(...)):
    user_doc = chat_collection.find_one({"user_id": user_id})
    if not user_doc or not user_doc.get("sessions"):
        raise HTTPException(status_code=404, detail="No history found for this user.")

    return user_doc["sessions"]


# ---- Run the App ----
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
