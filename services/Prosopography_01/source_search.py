"""Source search and fetch module for Prosopography_01.

Integrates with Serper API for Google search and fetches page content.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from dotenv import load_dotenv

load_dotenv()


class SourceSearcher:
    """Search and fetch sources for prosopography research."""

    def __init__(self):
        """Initialize the source searcher."""
        self.serper_api_key = os.getenv("SERPER_API_KEY")
        if not self.serper_api_key:
            raise ValueError("SERPER_API_KEY environment variable not set")

        self.search_endpoint = "https://google.serper.dev/search"
        self.user_agent = "EliteResearchAgent/1.0"

    def search(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Search Google via Serper API.

        Args:
            query: Search query string
            num_results: Number of results to return

        Returns:
            List of search results with url, title, snippet
        """
        headers = {
            "X-API-KEY": self.serper_api_key,
            "Content-Type": "application/json"
        }
        payload = {"q": query, "num": num_results}

        resp = requests.post(
            self.search_endpoint,
            json=payload,
            headers=headers,
            timeout=20
        )
        resp.raise_for_status()

        data = resp.json()
        results = []

        for item in data.get("organic", []):
            results.append({
                "url": item.get("link"),
                "title": item.get("title"),
                "snippet": item.get("snippet"),
            })

        return results

    def search_person_sources(
        self,
        person_name: str,
        additional_terms: Optional[List[str]] = None,
        num_results: int = 10,
        exclude_wikipedia: bool = True
    ) -> List[Dict[str, Any]]:
        """Search for sources about a specific person.

        Args:
            person_name: Name of the person to search for
            additional_terms: Optional list of additional search terms
            num_results: Number of results per query
            exclude_wikipedia: Whether to exclude Wikipedia from results

        Returns:
            List of unique search results
        """
        queries = [
            f'"{person_name}" biography career',
            f'"{person_name}" curriculum vitae CV',
        ]

        if additional_terms:
            for term in additional_terms:
                queries.append(f'"{person_name}" {term}')

        all_results = []
        seen_urls = set()

        for query in queries:
            try:
                results = self.search(query, num_results)
                for result in results:
                    url = result.get("url", "")

                    # Skip if already seen
                    if url in seen_urls:
                        continue

                    # Skip Wikipedia if requested
                    if exclude_wikipedia and "wikipedia.org" in url.lower():
                        continue

                    seen_urls.add(url)
                    all_results.append(result)

            except Exception as e:
                print(f"Search error for '{query}': {e}")

        return all_results

    def fetch_content(self, url: str, timeout: int = 15) -> Dict[str, Any]:
        """Fetch and extract text content from a URL.

        Args:
            url: URL to fetch
            timeout: Request timeout in seconds

        Returns:
            Dictionary with title, text, extraction_method, success, error
        """
        headers = {"User-Agent": self.user_agent}

        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()

            content_type = resp.headers.get('content-type', '').lower()

            # Handle PDFs
            if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
                return self._extract_pdf(resp.content, url)

            # Handle HTML
            return self._extract_html(resp.text, url)

        except RequestException as e:
            return {
                "url": url,
                "title": "",
                "text": "",
                "extraction_method": None,
                "success": False,
                "error": str(e)
            }

    def _extract_html(self, html: str, url: str) -> Dict[str, Any]:
        """Extract text from HTML content."""
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Get title
            title = ""
            if soup.title and soup.title.string:
                title = soup.title.string.strip()

            # Remove non-content elements
            for tag in soup(["script", "style", "noscript", "header", "footer",
                           "nav", "aside", "svg", "form", "button"]):
                tag.extract()

            # Extract text
            text = soup.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            text = "\n".join(lines)

            return {
                "url": url,
                "title": title,
                "text": text,
                "extraction_method": "html",
                "success": True,
                "error": None
            }

        except Exception as e:
            return {
                "url": url,
                "title": "",
                "text": "",
                "extraction_method": "html",
                "success": False,
                "error": str(e)
            }

    def _extract_pdf(self, content: bytes, url: str) -> Dict[str, Any]:
        """Extract text from PDF content."""
        try:
            import io
            try:
                import PyPDF2
            except ImportError:
                return {
                    "url": url,
                    "title": url.split('/')[-1],
                    "text": "[PDF content - PyPDF2 not installed]",
                    "extraction_method": "pdf_basic",
                    "success": False,
                    "error": "PyPDF2 not installed"
                }

            pdf_file = io.BytesIO(content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)

            text_parts = []
            for page in pdf_reader.pages:
                text_parts.append(page.extract_text())

            text = "\n\n".join(text_parts)
            title = url.split('/')[-1]

            return {
                "url": url,
                "title": title,
                "text": text,
                "extraction_method": "pdf_basic",
                "success": True,
                "error": None
            }

        except Exception as e:
            return {
                "url": url,
                "title": url.split('/')[-1],
                "text": "",
                "extraction_method": "pdf_basic",
                "success": False,
                "error": str(e)
            }

    def search_and_fetch(
        self,
        person_name: str,
        additional_terms: Optional[List[str]] = None,
        num_results: int = 10,
        exclude_wikipedia: bool = True,
        max_fetch: int = 5
    ) -> List[Dict[str, Any]]:
        """Search for sources and fetch their content.

        Args:
            person_name: Name of the person
            additional_terms: Additional search terms
            num_results: Number of search results
            exclude_wikipedia: Exclude Wikipedia results
            max_fetch: Maximum number of URLs to actually fetch

        Returns:
            List of sources with fetched content
        """
        # Search
        results = self.search_person_sources(
            person_name=person_name,
            additional_terms=additional_terms,
            num_results=num_results,
            exclude_wikipedia=exclude_wikipedia
        )

        # Fetch content for top results
        fetched = []
        for result in results[:max_fetch]:
            url = result.get("url")
            if not url:
                continue

            content = self.fetch_content(url)
            content["search_title"] = result.get("title")
            content["search_snippet"] = result.get("snippet")
            fetched.append(content)

        return fetched


def chunk_text(text: str, chunk_size: int = 4000, overlap: int = 200) -> List[str]:
    """Split text into overlapping chunks.

    Args:
        text: Text to split
        chunk_size: Target chunk size in characters
        overlap: Overlap between chunks

    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size

        # Try to break at sentence boundary
        if end < len(text):
            for punct in [". ", ".\n", "? ", "?\n", "! ", "!\n"]:
                last_punct = text.rfind(punct, start, end)
                if last_punct > start + chunk_size // 2:
                    end = last_punct + len(punct)
                    break

        chunks.append(text[start:end])
        start = end - overlap

    return chunks
