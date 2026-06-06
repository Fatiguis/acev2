import sqlite3
import sys

def console_login():
    # VULNERABILITY: Takes input directly from the command line execution
    if len(sys.argv) < 3:
        print("Usage: python script.py <username> <password>")
        return
        
    username = sys.argv[1]
    password = sys.argv[2]
    
    conn = sqlite3.connect('production.db')
    cursor = conn.cursor()
    
    # HIGH CRITICAL: Direct concatenation from sys.argv into the database
    query = "SELECT * FROM users WHERE username='" + username + "' AND password='" + password + "'"
    cursor.execute(query)
    
    if cursor.fetchone():
        print("Access Granted to Mainframe.")
    else:
        print("Access Denied.")

# Claude will see this and know the file is actively executable from the terminal!
if __name__ == "__main__":
    console_login()
