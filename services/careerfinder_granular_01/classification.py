from typing import Dict, Any

def classify_chunk(chunk: Dict[str, Any]) -> str:
    url = chunk.get("source_url", "").lower()
    title = chunk.get("title", "").lower()
    extraction_method = chunk.get("extraction_method", "")
    
    if extraction_method == "pdf_basic" and ("cv" in title or "curriculum" in title or "resume" in title or "vita" in title):
        return "cv_structured"
    
    if "wikipedia.org" in url or "wiki" in url:
        return "wikipedia_narrative"
    
    if extraction_method == "pdf_basic":
        return "pdf_document"
    
    if extraction_method == "html":
        return "web_general"
    
    return "unknown"

if __name__ == "__main__":
    test_chunks = [
        {
            "source_url": "https://economics.mit.edu/sites/default/files/2022-08/2022.08%20on%20website.pdf",
            "title": "2022.08%20on%20website.pdf",
            "extraction_method": "pdf_basic"
        },
        {
            "source_url": "https://en.wikipedia.org/wiki/Gro_Harlem_Brundtland",
            "title": "Gro Harlem Brundtland - Wikipedia",
            "extraction_method": "html"
        },
        {
            "source_url": "https://www.tse-fr.eu/sites/default/files/TSE/documents/doc/cv/tirole_en.pdf",
            "title": "tirole_en.pdf",
            "extraction_method": "pdf_basic"
        }
    ]
    
    for chunk in test_chunks:
        doc_type = classify_chunk(chunk)
        print(f"{chunk['title']}: {doc_type}")