import os
import requests
from dotenv import load_dotenv

load_dotenv()
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SEARCH_ENDPOINT = "https://google.serper.dev/search"

HEADERS = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

def search(query: str, num_results: int = 8) -> dict:
    payload = {"q": query, "num": num_results}
    resp = requests.post(SEARCH_ENDPOINT, json=payload, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()
