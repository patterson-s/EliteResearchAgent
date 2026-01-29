import argparse
from pathlib import Path

from load_events import load_events_from_chunks
from phase1a_discover_career_labels import discover_career_labels
from phase1b_discover_award_labels import discover_award_labels
from phase2a_classify_career_events import classify_career_events
from phase2b_classify_award_events import classify_award_events

def run_pipeline(data_dir: Path, config_path: Path, output_dir: Path, skip_phases: list = None):
    skip_phases = skip_phases or []
    
    print("\n" + "="*80)
    print("EVENTALIGN_03 - ABDUCTIVE CORE DISCOVERY PIPELINE")
    print("="*80)
    print(f"Data: {data_dir}")
    print(f"Config: {config_path}")
    print(f"Output: {output_dir}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nLoading events...")
    events = load_events_from_chunks(data_dir)
    
    print(f"  Total events: {len(events['all'])}")
    print(f"  Career positions: {len(events['career'])}")
    print(f"  Awards: {len(events['awards'])}")
    
    if 1 not in skip_phases:
        discover_career_labels(events["career"], config_path, output_dir)
    else:
        print("\nSkipping Phase 1a")
    
    if 2 not in skip_phases:
        discover_award_labels(events["awards"], config_path, output_dir)
    else:
        print("\nSkipping Phase 1b")
    
    if 3 not in skip_phases:
        import json
        with open(output_dir / "01a_career_labels.json", "r", encoding="utf-8") as f:
            career_labels_data = json.load(f)
        
        classify_career_events(
            events["career"],
            career_labels_data["career_labels"],
            config_path,
            output_dir
        )
    else:
        print("\nSkipping Phase 2a")
    
    if 4 not in skip_phases:
        import json
        with open(output_dir / "01b_award_labels.json", "r", encoding="utf-8") as f:
            award_labels_data = json.load(f)
        
        classify_award_events(
            events["awards"],
            award_labels_data["award_labels"],
            config_path,
            output_dir
        )
    else:
        print("\nSkipping Phase 2b")
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETE")
    print("="*80)
    print("\nGenerated files:")
    print(f"  1a. {output_dir / '01a_career_labels.json'}")
    print(f"  1b. {output_dir / '01b_award_labels.json'}")
    print(f"  2a. {output_dir / '02a_career_classifications.json'}")
    print(f"  2b. {output_dir / '02b_award_classifications.json'}")
    print("\nRun generate_report.py to create summary report")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run EventAlign_03 pipeline")
    parser.add_argument("--skip", nargs="+", type=int, choices=[1, 2, 3, 4],
                        help="Skip phases (1=discover career, 2=discover awards, 3=classify career, 4=classify awards)")
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data" / "Abhijit_Banerjee"
    config_path = script_dir / "config" / "config.json"
    output_dir = script_dir / "outputs"
    
    run_pipeline(data_dir, config_path, output_dir, args.skip)