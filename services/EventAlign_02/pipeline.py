import json
import argparse
from pathlib import Path

from normalize_entities import normalize_entities
from group_candidates import apply_normalization, group_candidates
from consolidate_llm import consolidate_with_llm
from build_timeline import build_timeline

def run_pipeline(data_dir: Path, output_dir: Path, config_path: Path, skip_phases: list = None):
    skip_phases = skip_phases or []
    
    print("\n" + "="*80)
    print("EVENT CONSOLIDATION PIPELINE")
    print("="*80)
    print(f"Data directory: {data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Config: {config_path}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    events_file = output_dir / "all_resolved_events.json"
    if not events_file.exists():
        print(f"\nERROR: {events_file} not found")
        print("Run explore_timeline.py first to generate all_resolved_events.json")
        return
    
    with open(events_file, "r", encoding="utf-8") as f:
        events = json.load(f)
    
    print(f"\nLoaded {len(events)} events")
    
    if 1 not in skip_phases:
        normalization_map = normalize_entities(events, output_dir, config_path)
    else:
        print("\nSkipping Phase 1 (loading existing normalization)")
        with open(output_dir / "01_normalized_entities.json", "r", encoding="utf-8") as f:
            normalization_map = json.load(f)
    
    if 2 not in skip_phases:
        normalized_events = apply_normalization(events, normalization_map)
        
        with open(output_dir / "all_normalized_events.json", "w", encoding="utf-8") as f:
            json.dump(normalized_events, f, indent=2, ensure_ascii=False)
        
        groups = group_candidates(normalized_events, output_dir, config_path)
    else:
        print("\nSkipping Phase 2 (loading existing groups)")
        with open(output_dir / "02_candidate_groups.json", "r", encoding="utf-8") as f:
            groups = json.load(f)
    
    if 3 not in skip_phases:
        consolidated_events = consolidate_with_llm(groups, output_dir, config_path)
    else:
        print("\nSkipping Phase 3 (loading existing consolidation)")
        with open(output_dir / "03_consolidated_events.json", "r", encoding="utf-8") as f:
            consolidated_events = json.load(f)
    
    if 4 not in skip_phases:
        timeline = build_timeline(consolidated_events, output_dir)
    else:
        print("\nSkipping Phase 4")
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETE")
    print("="*80)
    print("\nGenerated files:")
    print(f"  1. {output_dir / '01_normalized_entities.json'}")
    print(f"  2. {output_dir / '02_candidate_groups.json'}")
    print(f"  3. {output_dir / '03_consolidated_events.json'}")
    print(f"  4. {output_dir / '04_final_timeline.json'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run event consolidation pipeline")
    parser.add_argument("--skip", nargs="+", type=int, choices=[1, 2, 3, 4],
                        help="Skip phases (1=normalize, 2=group, 3=consolidate, 4=timeline)")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    output_dir = script_dir / "outputs"
    config_path = script_dir / "config" / "config.json"
    
    run_pipeline(data_dir, output_dir, config_path, args.skip)