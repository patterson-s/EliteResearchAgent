import json
import argparse
from pathlib import Path
from typing import List, Dict, Any
from collections import Counter

def load_verification_results(review_dir: Path) -> List[Dict[str, Any]]:
    results = []
    
    for json_file in review_dir.glob("birthyear_*.json"):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            results.append(data)
    
    return results

def generate_summary(results: List[Dict[str, Any]]) -> None:
    output_lines = []
    
    def print_and_log(text=""):
        print(text)
        output_lines.append(text)
    
    print_and_log("=" * 100)
    print_and_log("Birth Year Verification - Test Summary")
    print_and_log("=" * 100)
    print_and_log()
    
    print_and_log(f"Total people processed: {len(results)}")
    print_and_log()
    
    status_counts = Counter()
    verified_count = 0
    found_birth_year = 0
    
    print_and_log("Individual Results:")
    print_and_log("-" * 100)
    
    for result in sorted(results, key=lambda x: x["person_name"]):
        person = result["person_name"]
        verification = result["verification"]
        status = verification["verification_status"]
        birth_year = verification.get("birth_year")
        source_count = verification["independent_source_count"]
        extractions_count = verification["total_extractions"]
        
        status_counts[status] += 1
        
        if status in ["verified", "conflict_resolved"]:
            verified_count += 1
        
        if birth_year:
            found_birth_year += 1
        
        status_symbol = "✓" if status in ["verified", "conflict_resolved"] else "○"
        
        print_and_log(f"{status_symbol} {person}")
        print_and_log(f"   Status: {status}")
        print_and_log(f"   Birth year: {birth_year if birth_year else 'Not found'}")
        print_and_log(f"   Independent sources: {source_count}")
        print_and_log(f"   Extractions scanned: {extractions_count}")
        
        if verification.get("year_ledgers"):
            years_found = list(verification["year_ledgers"].keys())
            if len(years_found) > 1:
                print_and_log(f"   Conflict: Multiple years found: {years_found}")
        
        print_and_log()
    
    print_and_log("=" * 100)
    print_and_log("Summary Statistics")
    print_and_log("=" * 100)
    print_and_log()
    
    print_and_log(f"Verification achieved (2+ sources): {verified_count}/{len(results)} ({verified_count/len(results)*100:.1f}%)")
    print_and_log(f"Birth year found (any status):      {found_birth_year}/{len(results)} ({found_birth_year/len(results)*100:.1f}%)")
    print_and_log()
    
    print_and_log("Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print_and_log(f"  {status:25} {count:2}")
    
    print_and_log()
    print_and_log("=" * 100)
    
    return output_lines

def main():
    parser = argparse.ArgumentParser(
        description="Summarize birth year verification test results"
    )
    parser.add_argument(
        "--review-dir",
        type=Path,
        default=Path("review"),
        help="Directory containing verification JSON files"
    )
    
    args = parser.parse_args()
    
    if not args.review_dir.exists():
        print(f"Error: Review directory not found: {args.review_dir}")
        return
    
    results = load_verification_results(args.review_dir)
    
    if not results:
        print(f"No verification results found in {args.review_dir}")
        return
    
    output_lines = generate_summary(results)
    
    from datetime import datetime
    summary_file = args.review_dir / f"summary_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    
    print()
    print(f"Summary saved to: {summary_file}")

if __name__ == "__main__":
    main()