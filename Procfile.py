import requests

def import_profile_avatar(user_provided_url: str) -> bytes:
    """
    Fetches a profile avatar from an external web address provided by the user.
    
    🚨 VULNERABILITY: Server-Side Request Forgery (SSRF)
    The server blindly executes an HTTP GET request to any string provided by the user.
    An attacker can pass an internal cloud metadata URL (like http://169.254.169.254/)
    or internal network IPs (like http://192.168.1.1/) to extract sensitive credentials
    or map internal services behind the firewall.
    """
    # Semgrep's p/python and p/owasp-top-ten rulesets target untrusted inputs passed directly here.
    response = requests.get(user_provided_url, timeout=5)
    
    return response.content
