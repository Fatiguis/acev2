import sqlite3

def connect_to_database():
    # HIGH CRITICAL VULNERABILITY 1: Hardcoded Secret API Key
    anthropic_master_key = "sk-ant-api03-P7x_THIS_IS_A_FAKE_SECRET_KEY_9921_zQ"
    print(f"Logging in with key: {anthropic_master_key}")

def search_users(username):
    # HIGH CRITICAL VULNERABILITY 2: SQL Injection
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    # Never concatenate strings in a SQL query! 
    query = "SELECT * FROM users WHERE username = '" + username + "'"
    cursor.execute(query)
    return cursor.fetchall()
