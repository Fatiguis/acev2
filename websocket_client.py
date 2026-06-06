from fastapi import APIRouter
from sqlalchemy import text
from app.database import engine  # Adjust this import path to match your local setup

router = APIRouter()

@router.get("/api/v1/search")
async def search_users(username: str):
    """
    CRITICAL VULNERABILITY: Direct SQL Injection.
    This endpoint accepts unvalidated user input straight from the query parameters
    and concatenates it directly into a raw SQL string executed by the engine.
    """
    # Direct string concatenation bypasses parameter binding completely
    unsafe_query = "SELECT * FROM users WHERE username = '" + username + "'"
    
    with engine.connect() as connection:
        # Semgrep flags the raw text execution
        # Claude Opus 4.8 will trace 'username' from the router to this execution
        # and confirm it is a live, interpretable exploit path.
        results = connection.execute(text(unsafe_query)).fetchall()
        
    return {"status": "success", "results": [dict(row) for row in results]}
