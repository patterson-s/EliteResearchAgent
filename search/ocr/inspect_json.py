import json
import re
from pathlib import Path
from typing import List, Dict, Any

def has_garbled_text(text: str, threshold: float = 0.3) -> bool:
    if not text or len(text) < 10:
        return False
    
    non_printable = len(re.findall(r'[^\x20-\x7E\n\r\t]', text))
    ratio = non_printable / len(text)
    
    return ratio > threshold

def assess_extraction_quality(text: str) -> tuple[str, str]:
    if not text or text.startswith('[PDF'):
        return 'failed', 'Extraction returned error or empty result'
    
    clean_text = text.strip()
    
    if len(clean_text) < 100:
        return 'poor', f'Text too short ({len(clean_text)} characters)'
    
    if has_garbled_text(clean_text):
        return 'poor', 'High ratio of garbled/non-printable characters detected'
    
    return 'good', 'Extraction successful with clean text'

def inspect_json_results(input_file: Path, output_file: Path):
    with open(input_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"Loaded {len(results)} search results")
    
    needs_ocr_count = 0
    good_count = 0
    failed_count = 0
    
    for result in results:
        if result.get('fetch_status') != 'success':
            continue
        
        if result.get('extraction_method') != 'pdf_basic':
            continue
        
        full_text = result.get('full_text', '')
        quality, reason = assess_extraction_quality(full_text)
        
        result['extraction_quality'] = quality
        result['extraction_quality_reason'] = reason
        result['needs_ocr'] = quality in ('poor', 'failed')
        result['full_text_length'] = len(full_text) if full_text else 0
        
        if result['needs_ocr']:
            needs_ocr_count += 1
            print(f"  Needs OCR: {result.get('title', 'Untitled')[:50]} - {reason}")
        elif quality == 'good':
            good_count += 1
        else:
            failed_count += 1
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nInspection complete:")
    print(f"  Good extractions: {good_count}")
    print(f"  Need OCR: {needs_ocr_count}")
    print(f"  Failed: {failed_count}")
    print(f"Saved to: {output_file}")

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m search.ocr.inspect_json <input_json>")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    output_file = input_file.parent / f"{input_file.stem}_inspected.json"
    
    inspect_json_results(input_file, output_file)

if __name__ == "__main__":
    main()