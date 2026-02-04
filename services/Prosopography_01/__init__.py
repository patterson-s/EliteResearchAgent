"""
Prosopography Tool - Combined Career Event Extraction & Validation System

A unified web-based system for extracting, validating, and enriching career event data
from biographical sources.

Workflow:
1. Phase 1: Extract initial career events from Wikipedia (automated)
2. Phase 2: User reviews and corrects the template (human-in-the-loop)
3. Phase 3: Supplement with additional sources (automated with optional review)

Usage:
    # Run the Streamlit UI
    streamlit run ui/app.py

    # Or use programmatically:
    from phase1.pipeline import Phase1Pipeline
    from phase2.correction_service import CorrectionService
    from phase3.pipeline import Phase3Pipeline

    # Phase 1
    pipeline = Phase1Pipeline()
    result = pipeline.run("Person Name", wikipedia_text, source_url)

    # Phase 2
    correction_service = CorrectionService()
    correction_service.apply_correction(event_id, "roles", is_valid=False, corrected_value="New Role")

    # Phase 3
    pipeline = Phase3Pipeline()
    result = pipeline.process_source(person_id, chunks, source_url)
"""

__version__ = "1.0.0"
