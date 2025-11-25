import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json
from search.provenance.generate import generate_narratives_for_json

REVIEW_DIR = Path(__file__).parent / "review"

def run_step(description: str, command: list):
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"{'='*60}")
    
    result = subprocess.run(command, check=True)
    
    print(f"Completed: {description}\n")

def run_pipeline(names_file: Path, chunk_size: int = 400, ocr_limit: int = None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    serper_output_dir = Path("search/serper/outputs")
    serper_output_dir.mkdir(parents=True, exist_ok=True)
    
    step1_file = serper_output_dir / f"search_results_{timestamp}.json"
    step2_file = serper_output_dir / f"search_results_{timestamp}_inspected.json"
    step3_file = serper_output_dir / f"search_results_{timestamp}_inspected_ocr.json"
    step4_file = serper_output_dir / f"search_results_{timestamp}_inspected_ocr_chunked.json"
    step5_file = serper_output_dir / f"search_results_{timestamp}_inspected_ocr_chunked_embedded.json"
    
    print(f"\n{'='*60}")
    print(f"PIPELINE START")
    print(f"{'='*60}")
    print(f"Input: {names_file}")
    print(f"Timestamp: {timestamp}")
    print(f"Review output: {REVIEW_DIR}")
    
    run_step(
        "1. Search and Fetch",
        ["python", "-m", "search.serper.batch", str(names_file)]
    )
    
    run_step(
        "2. Inspect PDFs",
        ["python", "-m", "search.ocr.inspect_json", str(step1_file)]
    )
    
    ocr_cmd = ["python", "-m", "search.ocr.process_json", str(step2_file)]
    if ocr_limit:
        ocr_cmd.extend(["--limit", str(ocr_limit)])
    
    run_step(
        "3. OCR Processing",
        ocr_cmd
    )
    
    run_step(
        "4. Chunk Texts",
        ["python", "-m", "search.embeddings.chunk_json", str(step3_file), "--chunk-size", str(chunk_size)]
    )
    
    run_step(
        "5. Generate Embeddings",
        ["python", "-m", "search.embeddings.embed_json", str(step4_file)]
    )
    
    print(f"\n{'='*60}")
    print(f"STEP: 6. Generate Provenance Narratives")
    print(f"{'='*60}")
    
    with open(step5_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    results = generate_narratives_for_json(results)
    
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    final_file = REVIEW_DIR / f"search_complete_{timestamp}.json"
    
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"Generated provenance narratives")
    print(f"Completed: Generate Provenance Narratives\n")
    
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Final output: {final_file}")
    print(f"\nReady for review. When satisfied, load to database with:")
    print(f"  python -m search.load_review {final_file}")

def main():
    parser = argparse.ArgumentParser(description="Run complete search processing pipeline")
    parser.add_argument("names_file", type=Path, help="JSON file with list of person names")
    parser.add_argument("--chunk-size", type=int, default=400, help="Token count per chunk")
    parser.add_argument("--ocr-limit", type=int, default=None, help="Limit number of PDFs to OCR")
    
    args = parser.parse_args()
    
    if not args.names_file.exists():
        print(f"Error: File not found: {args.names_file}")
        return 1
    
    run_pipeline(args.names_file, args.chunk_size, args.ocr_limit)

if __name__ == "__main__":
    main()