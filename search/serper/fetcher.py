import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import io
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

USER_AGENT = "EliteResearchAgent/1.0"

def extract_pdf_text(content: bytes) -> str:
    if not PDF_AVAILABLE:
        return "[PDF content - PyPDF2 not installed]"
    
    try:
        pdf_file = io.BytesIO(content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text_parts = []
        for page in pdf_reader.pages:
            text_parts.append(page.extract_text())
        
        return "\n\n".join(text_parts)
    except Exception as e:
        return f"[PDF extraction failed: {str(e)}]"

def fetch_url_text(url: str, timeout: int = 10) -> tuple[str, str]:
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
    except RequestException as e:
        raise
    
    content_type = r.headers.get('content-type', '').lower()
    
    if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
        title = url.split('/')[-1]
        text = extract_pdf_text(r.content)
        return title, text

    soup = BeautifulSoup(r.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    
    for s in soup(["script", "style", "noscript", "header", "footer", "svg"]):
        s.extract()
    texts = soup.get_text(separator="\n")
    
    lines = [ln.strip() for ln in texts.splitlines() if ln.strip()]
    text = "\n".join(lines)
    return title, text