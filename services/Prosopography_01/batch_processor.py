"""Batch processor for running Phase 1 pipeline on multiple people in parallel."""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
import threading
import time

sys.path.insert(0, str(Path(__file__).parent))

from db import PersonRepository, get_db_connection
from phase1.pipeline import Phase1Pipeline
from source_search import SourceSearcher


class ProcessingStatus(Enum):
    PENDING = "pending"
    FETCHING_WIKIPEDIA = "fetching_wikipedia"
    RUNNING_PIPELINE = "running_pipeline"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PersonProcessingResult:
    """Result of processing a single person."""
    person_id: int
    person_name: str
    status: ProcessingStatus
    events_found: int = 0
    error_message: str = ""
    wikipedia_url: str = ""


@dataclass
class BatchProgress:
    """Tracks overall batch processing progress."""
    total: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    running: int = 0
    results: Dict[int, PersonProcessingResult] = field(default_factory=dict)
    is_running: bool = False
    should_stop: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update_result(self, result: PersonProcessingResult):
        with self.lock:
            self.results[result.person_id] = result
            if result.status == ProcessingStatus.COMPLETE:
                self.completed += 1
                self.running -= 1
            elif result.status == ProcessingStatus.FAILED:
                self.failed += 1
                self.running -= 1
            elif result.status == ProcessingStatus.SKIPPED:
                self.skipped += 1
                self.running -= 1
            elif result.status in [ProcessingStatus.FETCHING_WIKIPEDIA, ProcessingStatus.RUNNING_PIPELINE]:
                if result.person_id not in self.results or self.results[result.person_id].status == ProcessingStatus.PENDING:
                    self.running += 1

    def get_progress_pct(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.completed + self.failed + self.skipped) / self.total


class BatchProcessor:
    """Processes multiple people through Phase 1 pipeline in parallel."""

    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.person_repo = PersonRepository()
        self.progress = BatchProgress()

    def get_unprocessed_persons(self) -> List[Dict[str, Any]]:
        """Get all persons with 0 events and pending status."""
        summaries = self.person_repo.get_summary()
        unprocessed = [
            s for s in summaries
            if s.get("event_count", 0) == 0 and s.get("validated_count", 0) == 0
        ]
        return unprocessed

    def process_single_person(self, person_id: int, person_name: str) -> PersonProcessingResult:
        """Process a single person through Wikipedia fetch and Phase 1 pipeline."""
        result = PersonProcessingResult(
            person_id=person_id,
            person_name=person_name,
            status=ProcessingStatus.PENDING
        )

        # Check if we should stop
        if self.progress.should_stop:
            result.status = ProcessingStatus.SKIPPED
            result.error_message = "Batch processing stopped by user"
            return result

        try:
            # Step 1: Fetch Wikipedia
            result.status = ProcessingStatus.FETCHING_WIKIPEDIA
            self.progress.update_result(result)

            searcher = SourceSearcher()

            # Search for Wikipedia article
            search_results = searcher.search(
                f'"{person_name}" site:wikipedia.org',
                num_results=3
            )

            # Find best Wikipedia URL
            wiki_url = None
            for sr in search_results:
                url = sr.get("url", "")
                if "wikipedia.org/wiki/" in url and not any(
                    x in url for x in ["/File:", "/Category:", "/Template:", "/Talk:"]
                ):
                    wiki_url = url
                    break

            if not wiki_url:
                result.status = ProcessingStatus.SKIPPED
                result.error_message = "No Wikipedia article found"
                return result

            result.wikipedia_url = wiki_url

            # Fetch Wikipedia content
            content = searcher.fetch_content(wiki_url)
            if not content.get("success"):
                result.status = ProcessingStatus.FAILED
                result.error_message = f"Failed to fetch Wikipedia: {content.get('error', 'Unknown error')}"
                return result

            wikipedia_text = content.get("text", "")
            if not wikipedia_text or len(wikipedia_text) < 100:
                result.status = ProcessingStatus.SKIPPED
                result.error_message = "Wikipedia article too short or empty"
                return result

            # Check again if we should stop
            if self.progress.should_stop:
                result.status = ProcessingStatus.SKIPPED
                result.error_message = "Batch processing stopped by user"
                return result

            # Step 2: Run Phase 1 Pipeline
            result.status = ProcessingStatus.RUNNING_PIPELINE
            self.progress.update_result(result)

            pipeline = Phase1Pipeline()
            pipeline_result = pipeline.run(
                person_name=person_name,
                wikipedia_text=wikipedia_text,
                source_url=wiki_url,
                save_checkpoints=True
            )

            # Extract results
            events_count = pipeline_result.get("steps", {}).get("step3", {}).get("events_count", 0)
            result.events_found = events_count
            result.status = ProcessingStatus.COMPLETE

        except Exception as e:
            result.status = ProcessingStatus.FAILED
            result.error_message = str(e)

        return result

    def run_batch(
        self,
        persons: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[BatchProgress], None]] = None
    ) -> BatchProgress:
        """Run batch processing on a list of persons.

        Args:
            persons: List of person dicts with person_id and person_name
            progress_callback: Optional callback called after each person completes

        Returns:
            BatchProgress with final results
        """
        self.progress = BatchProgress(
            total=len(persons),
            is_running=True
        )

        # Initialize all as pending
        for p in persons:
            self.progress.results[p["person_id"]] = PersonProcessingResult(
                person_id=p["person_id"],
                person_name=p["person_name"],
                status=ProcessingStatus.PENDING
            )

        if not persons:
            self.progress.is_running = False
            return self.progress

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self.process_single_person,
                    p["person_id"],
                    p["person_name"]
                ): p["person_id"]
                for p in persons
            }

            # Process as they complete
            for future in as_completed(futures):
                person_id = futures[future]
                try:
                    result = future.result()
                    self.progress.update_result(result)
                except Exception as e:
                    # Handle unexpected errors
                    person_name = next(
                        (p["person_name"] for p in persons if p["person_id"] == person_id),
                        "Unknown"
                    )
                    result = PersonProcessingResult(
                        person_id=person_id,
                        person_name=person_name,
                        status=ProcessingStatus.FAILED,
                        error_message=f"Unexpected error: {str(e)}"
                    )
                    self.progress.update_result(result)

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(self.progress)

                # Check if we should stop
                if self.progress.should_stop:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

        self.progress.is_running = False
        return self.progress

    def stop(self):
        """Signal the batch processor to stop."""
        self.progress.should_stop = True
