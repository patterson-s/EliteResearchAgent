import json
import time
from pathlib import Path
from datetime import datetime
from search.ocr.mistral import MistralOCR

OUTPUT_DIR = Path(__file__).parent / "outputs"

def process_json_ocr(input_file: Path, output_file: Path, limit: int = None):
    with open(input_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    ocr = MistralOCR()
    
    pdfs_to_process = [r for r in results if r.get('needs_ocr') == True]
    
    if limit:
        pdfs_to_process = pdfs_to_process[:limit]
    
    print(f"Found {len(pdfs_to_process)} PDFs needing OCR")
    
    if not pdfs_to_process:
        print("No PDFs to process")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        return
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    processed_count = 0
    failed_count = 0
    
    for idx, result in enumerate(results, 1):
        if not result.get('needs_ocr'):
            continue
        
        if limit and processed_count >= limit:
            break
        
        print(f"\n[{processed_count + 1}/{len(pdfs_to_process)}] Processing: {result.get('person')} - {result.get('title', 'Untitled')[:50]}")
        print(f"  URL: {result.get('url')}")
        
        try:
            markdown_text = ocr.ocr_pdf_from_url(result['url'], OUTPUT_DIR / "temp")
            
            result['full_text'] = markdown_text
            result['full_text_length'] = len(markdown_text)
            result['extraction_method'] = 'pdf_ocr'
            result['extraction_quality'] = 'good'
            result['extraction_quality_reason'] = 'OCR processing successful'
            result['needs_ocr'] = False
            result['ocr_processed_at'] = datetime.utcnow().isoformat()
            
            print(f"  Updated with OCR text ({len(markdown_text)} characters)")
            processed_count += 1
            
        except Exception as e:
            print(f"  OCR failed: {e}")
            result['extraction_quality'] = 'failed'
            result['extraction_quality_reason'] = f'OCR processing failed: {str(e)}'
            failed_count += 1
        
        if processed_count < len(pdfs_to_process):
            time.sleep(2)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nComplete!")
    print(f"  Processed: {processed_count}")
    print(f"  Failed: {failed_count}")
    print(f"Saved to: {output_file}")

def main():
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Process PDFs with Mistral OCR on JSON file")
    parser.add_argument("input_file", type=Path, help="Input JSON file")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of PDFs to process")
    
    args = parser.parse_args()
    
    output_file = args.input_file.parent / f"{args.input_file.stem}_ocr.json"
    
    process_json_ocr(args.input_file, output_file, args.limit)

if __name__ == "__main__":
    main()