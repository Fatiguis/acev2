import sqlite3

def get_wallet_balance(wallet_id: str) -> float:
    """
    Fetches the current balance for a given wallet ID.
    """
    conn = sqlite3.connect("wallets.db")
    cursor = conn.cursor()
    
    # 🚨 VULNERABILITY: SQL Injection
    # We are directly injecting user input into the SQL query using an f-string.
    # An attacker could pass a wallet_id like: 123' OR '1'='1
    # This would force the database to return everyone's wallet balances.
    query = f"SELECT balance FROM wallets WHERE id = '{wallet_id}'"
    
    cursor.execute(query)
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return float(result[0])
    return 0.0
