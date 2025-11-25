from datetime import datetime
from typing import Dict, Any

def format_timestamp(iso_string: str) -> str:
    if not iso_string:
        return "unknown time"
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except:
        return iso_string

def generate_narrative(result: Dict[str, Any]) -> str:
    parts = []
    
    person = result.get('person', 'Unknown')
    search_query = result.get('search_query', '')
    searched_at = format_timestamp(result.get('searched_at'))
    rank = result.get('rank', 0)
    url = result.get('url', '')
    title = result.get('title', 'Untitled')
    
    parts.append(f"This source was collected on {searched_at} as part of a search for \"{person}\".")
    parts.append(f"The search query was: {search_query}")
    parts.append(f"This URL ranked #{rank} in the search results.")
    parts.append(f"URL: {url}")
    parts.append(f"Title: {title}")
    
    fetch_status = result.get('fetch_status')
    if fetch_status == 'success':
        fetched_at = format_timestamp(result.get('fetched_at'))
        parts.append(f"The document was fetched successfully on {fetched_at}.")
    else:
        fetch_error = result.get('fetch_error', 'Unknown error')
        parts.append(f"Fetch failed: {fetch_error}")
        return "\n".join(parts)
    
    extraction_method = result.get('extraction_method', 'unknown')
    parts.append(f"Initial extraction method: {extraction_method}")
    
    extraction_quality = result.get('extraction_quality')
    if extraction_quality:
        reason = result.get('extraction_quality_reason', '')
        parts.append(f"Extraction quality assessment: {extraction_quality}")
        if reason:
            parts.append(f"Reason: {reason}")
    
    if result.get('ocr_processed_at'):
        ocr_processed_at = format_timestamp(result.get('ocr_processed_at'))
        parts.append(f"OCR processing applied on {ocr_processed_at} using Mistral OCR.")
        parts.append(f"Final extraction method: {result.get('extraction_method')}")
    
    full_text_length = result.get('full_text_length', 0)
    parts.append(f"The full text contains {full_text_length:,} characters.")
    
    if result.get('chunks'):
        num_chunks = len(result['chunks'])
        chunked_at = format_timestamp(result.get('chunked_at'))
        chunk_size = result.get('chunk_size', 'unknown')
        parts.append(f"This source was chunked into {num_chunks} text segments on {chunked_at} (chunk size: {chunk_size} tokens).")
        
        if result['chunks'] and result['chunks'][0].get('embedding'):
            embedded_at = format_timestamp(result['chunks'][0].get('embedded_at'))
            embedding_model = result['chunks'][0].get('embedding_model', 'unknown')
            parts.append(f"Embeddings generated on {embedded_at} using Cohere {embedding_model}.")
    
    return "\n".join(parts)

def generate_narratives_for_json(results: list) -> list:
    for result in results:
        result['provenance_narrative'] = generate_narrative(result)
    return results