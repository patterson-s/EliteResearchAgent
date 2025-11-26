from typing import Dict, List, Any, Optional

QUALITY_RANKS = {
    "born-field": 0,
    "born-narrative": 1,
    "other": 2,
    "category": 3
}

def get_quality_rank(evidence_type: Optional[str]) -> int:
    return QUALITY_RANKS.get(evidence_type, 4)

class YearLedger:
    def __init__(self, year: int):
        self.year = year
        self.count = 0
        self.domains = set()
        self.extractions = []
    
    def add_extraction(self, extraction: Dict[str, Any], domain: str):
        if domain not in self.domains:
            self.count += 1
            self.domains.add(domain)
        
        self.extractions.append({
            "chunk_id": extraction["chunk_id"],
            "domain": domain,
            "url": extraction.get("url"),
            "evidence_type": extraction.get("evidence_type"),
            "quality_rank": get_quality_rank(extraction.get("evidence_type"))
        })
    
    def best_quality(self) -> int:
        if not self.extractions:
            return 999
        return min(e["quality_rank"] for e in self.extractions)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "year": self.year,
            "count": self.count,
            "domains": list(self.domains),
            "extractions": self.extractions
        }

def verify_birth_year(
    extractions: List[Dict[str, Any]],
    min_independent_sources: int = 2
) -> Dict[str, Any]:
    if not extractions:
        return {
            "verification_status": "no_evidence",
            "birth_year": None,
            "independent_source_count": 0,
            "total_extractions": 0,
            "year_ledgers": {}
        }
    
    year_ledgers: Dict[int, YearLedger] = {}
    
    for extraction in extractions:
        if not extraction.get("contains_birth_info"):
            continue
        
        year = extraction.get("extracted_year")
        if year is None:
            continue
        
        domain = extraction.get("domain", "")
        
        if year not in year_ledgers:
            year_ledgers[year] = YearLedger(year)
        
        year_ledgers[year].add_extraction(extraction, domain)
    
    if not year_ledgers:
        return {
            "verification_status": "no_evidence",
            "birth_year": None,
            "independent_source_count": 0,
            "total_extractions": len(extractions),
            "year_ledgers": {}
        }
    
    max_count = max(ledger.count for ledger in year_ledgers.values())
    top_years = [
        year for year, ledger in year_ledgers.items()
        if ledger.count == max_count
    ]
    
    if max_count >= min_independent_sources and len(top_years) == 1:
        winner = top_years[0]
        status = "verified" if len(year_ledgers) == 1 else "conflict_resolved"
        return {
            "verification_status": status,
            "birth_year": winner,
            "independent_source_count": year_ledgers[winner].count,
            "total_extractions": len(extractions),
            "year_ledgers": {y: ledger.to_dict() for y, ledger in year_ledgers.items()}
        }
    
    if len(top_years) > 1:
        best_year = None
        best_quality = 999
        
        for year in top_years:
            quality = year_ledgers[year].best_quality()
            if quality < best_quality:
                best_quality = quality
                best_year = year
        
        if best_year and year_ledgers[best_year].count >= min_independent_sources:
            return {
                "verification_status": "conflict_resolved",
                "birth_year": best_year,
                "independent_source_count": year_ledgers[best_year].count,
                "total_extractions": len(extractions),
                "year_ledgers": {y: ledger.to_dict() for y, ledger in year_ledgers.items()}
            }
        else:
            return {
                "verification_status": "conflict_inconclusive",
                "birth_year": best_year,
                "independent_source_count": year_ledgers[best_year].count if best_year else 0,
                "total_extractions": len(extractions),
                "year_ledgers": {y: ledger.to_dict() for y, ledger in year_ledgers.items()}
            }
    
    if len(year_ledgers) == 1:
        winner = next(iter(year_ledgers.keys()))
        return {
            "verification_status": "no_corroboration",
            "birth_year": winner,
            "independent_source_count": year_ledgers[winner].count,
            "total_extractions": len(extractions),
            "year_ledgers": {y: ledger.to_dict() for y, ledger in year_ledgers.items()}
        }
    
    return {
        "verification_status": "partial",
        "birth_year": None,
        "independent_source_count": max_count,
        "total_extractions": len(extractions),
        "year_ledgers": {y: ledger.to_dict() for y, ledger in year_ledgers.items()}
    }

if __name__ == "__main__":
    test_extractions = [
        {
            "chunk_id": 1,
            "contains_birth_info": True,
            "extracted_year": 1950,
            "evidence_type": "born-field",
            "domain": "wikipedia.org",
            "url": "https://en.wikipedia.org/wiki/Test"
        },
        {
            "chunk_id": 2,
            "contains_birth_info": True,
            "extracted_year": 1950,
            "evidence_type": "born-narrative",
            "domain": "britannica.com",
            "url": "https://www.britannica.com/biography/Test"
        },
        {
            "chunk_id": 3,
            "contains_birth_info": True,
            "extracted_year": 1951,
            "evidence_type": "category",
            "domain": "example.com",
            "url": "https://example.com/bio"
        }
    ]
    
    result = verify_birth_year(test_extractions, min_independent_sources=2)
    
    print("\n" + "=" * 80)
    print("Verification Test Result")
    print("=" * 80)
    print(f"Status: {result['verification_status']}")
    print(f"Birth year: {result['birth_year']}")
    print(f"Independent sources: {result['independent_source_count']}")
    print(f"Total extractions: {result['total_extractions']}")
    print("\nYear ledgers:")
    for year, ledger in result['year_ledgers'].items():
        print(f"  {year}: {ledger['count']} sources from {ledger['domains']}")