# Phase 1: Initial extraction from Wikipedia
# Import directly from submodules when needed:
# from phase1.pipeline import Phase1Pipeline
# from phase1.extract_entities import extract_entities_from_chunk
# etc.

__all__ = [
    'Phase1Pipeline',
    'extract_entities_from_chunk',
    'extract_entities_parallel',
    'discover_canonical_orgs',
    'build_org_mapping',
    'assemble_events',
    'verify_events'
]
