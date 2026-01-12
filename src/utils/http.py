import requests

def get_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def http_get_text(session: requests.Session, url: str, *, timeout: float = 15.0) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text
