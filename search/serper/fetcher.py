import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

USER_AGENT = "EliteResearchAgent/1.0"

def fetch_url_text(url: str, timeout: int = 10) -> tuple[str, str]:
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
    except RequestException as e:
        raise

    soup = BeautifulSoup(r.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    
    for s in soup(["script", "style", "noscript", "header", "footer", "svg"]):
        s.extract()
    texts = soup.get_text(separator="\n")
    
    lines = [ln.strip() for ln in texts.splitlines() if ln.strip()]
    text = "\n".join(lines)
    return title, text
