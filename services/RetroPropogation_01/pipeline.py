from pathlib import Path
from typing import Dict, Any

from load_data import load_chunks_for_person, get_wikipedia_source
from step1_extract_entities import extract_entities_from_chunks
from step2_discover_canonical_orgs import discover_canonical_orgs
from step3_assemble_events import assemble_events
from step4_verify_events import verify_events
from utils import save_json

def run_pipeline(person_name: str, config_path: Path, output_dir: Path) -> Dict[str, Any]:
    print("\n" + "="*80)
    print(f"RETROPROPAGATION_01 PIPELINE - {person_name}")
    print("="*80)
    
    print("\nLoading data from database...")
    chunks = load_chunks_for_person(person_name)
    print(f"Found {len(chunks)} total chunks")
    
    wiki_chunks = get_wikipedia_source(chunks)
    if not wiki_chunks:
        raise ValueError("No Wikipedia source found")
    
    print(f"Using Wikipedia source: {len(wiki_chunks)} chunks")
    total_chars = sum(len(c['text']) for c in wiki_chunks)
    print(f"Total text: {total_chars} characters")
    
    step1_result = extract_entities_from_chunks(wiki_chunks, config_path)
    save_json(step1_result, output_dir / "step1_entities.json")
    
    step2_result = discover_canonical_orgs(step1_result["entities"], config_path)
    save_json(step2_result, output_dir / "step2_canonical_orgs.json")
    
    step3_result = assemble_events(
        step1_result["entities"],
        step2_result["canonical_organizations"],
        config_path
    )
    save_json(step3_result, output_dir / "step3_events.json")
    
    step4_result = verify_events(
        step3_result["events"],
        step1_result["entities"],
        step3_result["deduplication_log"],
        config_path
    )
    save_json(step4_result, output_dir / "step4_verification.json")
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETE")
    print("="*80)
    print(f"\nOutput directory: {output_dir}")
    print("\nGenerated files:")
    print("  1. step1_entities.json")
    print("  2. step2_canonical_orgs.json")
    print("  3. step3_events.json")
    print("  4. step4_verification.json")
    
    summary = {
        "person_name": person_name,
        "source": "Wikipedia",
        "total_chunks": len(wiki_chunks),
        "total_characters": total_chars,
        "entities_extracted": {
            "time_markers": len(step1_result["entities"].get("time_markers", [])),
            "organizations": len(step1_result["entities"].get("organizations", [])),
            "roles": len(step1_result["entities"].get("roles", [])),
            "locations": len(step1_result["entities"].get("locations", []))
        },
        "canonical_organizations": len(step2_result["canonical_organizations"]),
        "events_assembled": len(step3_result["events"]),
        "verification_summary": step4_result["summary"]
    }
    
    save_json(summary, output_dir / "pipeline_summary.json")
    print("\n  5. pipeline_summary.json")
    
    return summary

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config" / "config.json"
    
    person_name = "Gro Harlem Brundtland"
    output_dir = script_dir / "outputs" / person_name.replace(" ", "_")
    
    summary = run_pipeline(person_name, config_path, output_dir)
    
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"\nExtracted {summary['entities_extracted']['time_markers']} time markers")
    print(f"Extracted {summary['entities_extracted']['organizations']} organization mentions")
    print(f"Consolidated into {summary['canonical_organizations']} canonical organizations")
    print(f"Assembled {summary['events_assembled']} career events")
    print(f"\nVerification: {summary['verification_summary']['valid']} valid, " 
          f"{summary['verification_summary']['warnings']} warnings, "
          f"{summary['verification_summary']['errors']} errors")