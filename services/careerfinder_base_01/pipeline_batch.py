import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import argparse

from pipeline import run_pipeline

class BatchState:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state = self._load_or_create()
    
    def _load_or_create(self) -> Dict[str, Any]:
        if self.state_file.exists():
            with open(self.state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None
    
    def create_new(self, chunks_file: Path, people: List[str]) -> None:
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.state = {
            "batch_id": batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "chunks_file": str(chunks_file),
            "total_people": len(people),
            "people": {
                person: {"status": "pending"}
                for person in people
            }
        }
        self.save()
    
    def save(self) -> None:
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
    
    def get_pending_people(self) -> List[str]:
        pending = []
        for person, data in self.state["people"].items():
            status = data["status"]
            if status == "pending":
                pending.append(person)
            elif status == "failed":
                retry_count = data.get("retry_count", 0)
                if retry_count < 2:
                    pending.append(person)
        return pending
    
    def mark_processing(self, person: str) -> None:
        self.state["people"][person]["status"] = "processing"
        self.save()
    
    def mark_complete(self, person: str, output_file: str, events_count: int) -> None:
        self.state["people"][person].update({
            "status": "complete",
            "output_file": output_file,
            "events_extracted": events_count,
            "completed_at": datetime.utcnow().isoformat()
        })
        self.save()
    
    def mark_failed(self, person: str, error: str) -> None:
        person_data = self.state["people"][person]
        retry_count = person_data.get("retry_count", 0) + 1
        
        if retry_count >= 2:
            person_data["status"] = "failed_permanent"
        else:
            person_data["status"] = "failed"
        
        person_data.update({
            "retry_count": retry_count,
            "error": str(error),
            "failed_at": datetime.utcnow().isoformat()
        })
        self.save()
    
    def get_summary(self) -> Dict[str, int]:
        summary = {
            "total": self.state["total_people"],
            "complete": 0,
            "failed": 0,
            "failed_permanent": 0,
            "pending": 0,
            "processing": 0
        }
        
        for data in self.state["people"].values():
            status = data["status"]
            if status in summary:
                summary[status] += 1
        
        return summary

def process_person(
    person_name: str,
    chunks_file: Path,
    config_path: Path,
    output_dir: Path,
    api_delay: float
) -> Dict[str, Any]:
    try:
        result = run_pipeline(
            person_name=person_name,
            config_path=config_path,
            output_dir=output_dir,
            from_file=chunks_file
        )
        
        if api_delay > 0:
            time.sleep(api_delay)
        
        return {
            "person": person_name,
            "status": "success",
            "events_count": result["events_extracted"],
            "output_file": None
        }
    
    except Exception as e:
        return {
            "person": person_name,
            "status": "error",
            "error": str(e)
        }

def load_people_from_chunks(chunks_file: Path) -> List[str]:
    with open(chunks_file, "r", encoding="utf-8") as f:
        chunks = json.load(f)
    
    people = sorted(set(chunk["person_name"] for chunk in chunks))
    return people

def run_batch(
    chunks_file: Path,
    config_path: Path,
    output_dir: Path,
    workers: int,
    api_delay: float,
    state_file: Optional[Path] = None,
    resume: bool = False
) -> None:
    
    if resume and state_file and state_file.exists():
        batch_state = BatchState(state_file)
        print(f"Resuming batch: {batch_state.state['batch_id']}")
    else:
        people = load_people_from_chunks(chunks_file)
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        state_file = output_dir / f"batch_state_{batch_id}.json"
        batch_state = BatchState(state_file)
        batch_state.create_new(chunks_file, people)
        print(f"Starting new batch: {batch_id}")
    
    pending = batch_state.get_pending_people()
    
    if not pending:
        print("No pending people to process")
        show_status(state_file)
        return
    
    print(f"\n{'=' * 100}")
    print(f"Batch Processing: {len(pending)} people with {workers} workers")
    print(f"{'=' * 100}\n")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    total_events = 0
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        
        for person in pending:
            batch_state.mark_processing(person)
            future = executor.submit(
                process_person,
                person,
                chunks_file,
                config_path,
                output_dir,
                api_delay
            )
            futures[future] = person
        
        completed = 0
        for future in as_completed(futures):
            person = futures[future]
            result = future.result()
            
            if result["status"] == "success":
                output_files = sorted(output_dir.glob(f"careerfinder_base_{person.replace(' ', '_')}_*.json"))
                if output_files:
                    latest_file = str(output_files[-1].name)
                    batch_state.mark_complete(person, latest_file, result["events_count"])
                    total_events += result["events_count"]
                    
                    completed += 1
                    percent = (completed / len(pending)) * 100
                    elapsed = time.time() - start_time
                    rate_people = (completed / elapsed) * 3600 if elapsed > 0 else 0
                    rate_events = (total_events / elapsed) * 3600 if elapsed > 0 else 0
                    
                    print(f"[{completed}/{len(pending)} | {percent:.1f}%] ✓ {person}: {result['events_count']} events | "
                          f"Rate: {rate_people:.1f} people/hr, {rate_events:.0f} events/hr")
                else:
                    batch_state.mark_failed(person, "Output file not found")
                    completed += 1
                    percent = (completed / len(pending)) * 100
                    print(f"[{completed}/{len(pending)} | {percent:.1f}%] ✗ {person}: Output file not found")
            else:
                batch_state.mark_failed(person, result["error"])
                retry_info = batch_state.state["people"][person]
                retry_count = retry_info.get("retry_count", 0)
                completed += 1
                percent = (completed / len(pending)) * 100
                print(f"[{completed}/{len(pending)} | {percent:.1f}%] ✗ {person}: {result['error']} (retry {retry_count}/2)")
    
    print(f"\n{'=' * 100}")
    print("Batch Complete")
    print(f"{'=' * 100}\n")
    
    show_status(state_file)

def show_status(state_file: Path) -> None:
    if not state_file.exists():
        print(f"State file not found: {state_file}")
        return
    
    batch_state = BatchState(state_file)
    summary = batch_state.get_summary()
    
    print(f"\nBatch ID: {batch_state.state['batch_id']}")
    print(f"Started: {batch_state.state['started_at']}")
    print(f"\nStatus Summary:")
    print(f"  Total:             {summary['total']}")
    print(f"  Complete:          {summary['complete']}")
    print(f"  Failed (retry):    {summary['failed']}")
    print(f"  Failed (permanent): {summary['failed_permanent']}")
    print(f"  Pending:           {summary['pending']}")
    print(f"  Processing:        {summary['processing']}")
    
    if summary['failed'] > 0:
        print(f"\nFailed people (will retry):")
        for person, data in batch_state.state["people"].items():
            if data["status"] == "failed":
                retry_count = data.get("retry_count", 0)
                print(f"  - {person} (retry {retry_count}/2): {data.get('error', 'Unknown')}")
    
    if summary['failed_permanent'] > 0:
        print(f"\nPermanently failed people:")
        for person, data in batch_state.state["people"].items():
            if data["status"] == "failed_permanent":
                print(f"  - {person}: {data.get('error', 'Unknown')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch process career extraction for multiple people"
    )
    
    parser.add_argument(
        "--chunks",
        type=Path,
        default=Path("data/all_chunks.json"),
        help="Path to chunks file"
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.json"),
        help="Path to config file"
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("review"),
        help="Output directory"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers"
    )
    
    parser.add_argument(
        "--api-delay",
        type=float,
        default=0.2,
        help="Delay between API calls per worker (seconds)"
    )
    
    parser.add_argument(
        "--resume",
        type=Path,
        help="Resume from batch state file"
    )
    
    parser.add_argument(
        "--status",
        type=Path,
        help="Show status of batch state file"
    )
    
    args = parser.parse_args()
    
    if args.status:
        show_status(args.status)
    elif args.resume:
        run_batch(
            chunks_file=args.chunks,
            config_path=args.config,
            output_dir=args.output,
            workers=args.workers,
            api_delay=args.api_delay,
            state_file=args.resume,
            resume=True
        )
    else:
        run_batch(
            chunks_file=args.chunks,
            config_path=args.config,
            output_dir=args.output,
            workers=args.workers,
            api_delay=args.api_delay
        )