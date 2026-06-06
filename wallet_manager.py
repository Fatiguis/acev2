import os
from fastapi import FastAPI

app = FastAPI()

# ⚠️ VULNERABILITY 1: Hardcoded Production Secrets
# Semgrep Rule: generic.secrets.gcp-service-account / aws-secret-access-key
# Scanners have high-entropy regex patterns specifically looking for these strings.
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
GCP_SERVICE_ACCOUNT_KEY = '{"type": "service_account", "project_id": "prod-db-admin", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3oV\\n-----END PRIVATE KEY-----\\n"}'

@app.get("/api/v1/debug")
def debug_endpoint(user_controlled_input: str):
    """
    CRITICAL INJECTION ROUTE
    """
    # ⚠️ VULNERABILITY 2: Absolute Direct Execution
    # Semgrep Rule: python.lang.security.audit.exec-eval
    # This runs whatever the user types directly into the core Python interpreter.
    exec(user_controlled_input)
    
    # ⚠️ VULNERABILITY 3: Dangerous File Permissions
    # Semgrep Rule: python.lang.security.audit.chmod-rwxt
    # Giving universal read/write/execute permissions to a system folder.
    os.chmod("/tmp/shared_cache", 0o777)
    
    return {"status": "debug_complete"}
