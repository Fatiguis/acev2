import sqlite3
import uvicorn
from fastapi import FastAPI

# 1. The AI sees the web server being created
app = FastAPI()

# 2. The AI sees an active route exposed to the public internet
@app.get("/api/search")
def public_search(user_query: str):
    conn = sqlite3.connect('production_data.db')
    cursor = conn.cursor()
    
    # 3. HIGH CRITICAL: Direct internet input concatenated into raw SQL
    unsafe_sql = "SELECT * FROM users WHERE username = '" + user_query + "'"
    cursor.execute(unsafe_sql)
    
    return cursor.fetchall()

# 4. The AI sees the server actively turning on and listening to the outside world
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
