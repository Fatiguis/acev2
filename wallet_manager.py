import os
import uvicorn
from fastapi import FastAPI

app = FastAPI()

@app.get("/api/network/ping")
def ping_external_server(ip_address: str):
    """
    VULNERABLE APP TARGET: Remote Command Injection (RCE)
    A classic CTF / PayloadsAllTheThings target.
    If a user inputs '8.8.8.8; cat /etc/passwd' or '1.1.1.1 & env',
    the server will execute the malicious command directly on the host OS.
    """
    # HIGH CRITICAL: Taking unvalidated internet input and passing it directly to the OS shell.
    command = "ping -c 1 " + ip_address
    
    # Executing the raw command and reading the output
    terminal_output = os.popen(command).read()
    
    return {"status": "executed", "terminal_output": terminal_output}

# Binding it to the internet so the AI knows it's a live web server
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
