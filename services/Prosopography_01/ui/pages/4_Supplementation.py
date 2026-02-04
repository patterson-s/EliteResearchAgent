"""Supplementation page for Phase 3 source enrichment."""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, EventRepository, get_connection, release_connection
from phase3.pipeline import Phase3Pipeline

st.set_page_config(page_title="Supplementation", layout="wide")
st.title("Supplementation (Phase 3)")

st.markdown("""
Enrich career events with additional sources. Events can be validated (confirmed by multiple sources),
supplemented (additional details added), or new events can be created.
""")

# Initialize
person_repo = PersonRepository()
event_repo = EventRepository()

# Person selection
persons = person_repo.get_all()
eligible = [p for p in persons if p.workflow_status in ["phase2_reviewed", "phase3_in_progress", "complete"]]

if not eligible:
    st.warning("No persons ready for supplementation. Complete Phase 2 review first.")
    st.stop()

person_names = [p.person_name for p in eligible]
selected_person = st.selectbox("Select Person", person_names)
person = person_repo.get_by_name(selected_person)

if not person:
    st.stop()

# Show current events summary
events = event_repo.get_for_person(person.person_id)
events = [e for e in events if e.validation_status != "rejected"]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Base Events", len(events))
with col2:
    validated = sum(1 for e in events if e.validation_status == "validated")
    st.metric("Validated", validated)
with col3:
    st.metric("Status", person.workflow_status)

st.markdown("---")

# Source selection
st.subheader("Available Sources")

# Get processed sources
conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_url, source_type, chunks_processed, events_merged, events_created
            FROM prosopography.sources_processed
            WHERE person_id = %s
        """, (person.person_id,))
        processed = {row[0]: row for row in cur.fetchall()}
finally:
    release_connection(conn)

# Get available sources from database
available_sources = []
try:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT c.source_url, c.title, COUNT(*) as chunk_count
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.source_url = sr.url
            JOIN sources.persons_searched ps ON sr.person_id = ps.person_id
            WHERE ps.person_name = %s
            GROUP BY c.source_url, c.title
        """, (selected_person,))
        for row in cur.fetchall():
            source_url = row[0]
            is_processed = source_url in processed
            available_sources.append({
                "source_url": source_url,
                "title": row[1] or source_url[:50],
                "chunk_count": row[2],
                "processed": is_processed,
                "stats": processed.get(source_url)
            })
    release_connection(conn)
except Exception as e:
    st.warning(f"Could not load sources: {e}")

# Display sources
if available_sources:
    for source in available_sources:
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            if source["processed"]:
                st.markdown(f"✅ **{source['title'][:60]}**")
            else:
                st.markdown(f"⬜ **{source['title'][:60]}**")
            st.caption(source["source_url"][:80])

        with col2:
            st.markdown(f"{source['chunk_count']} chunks")

        with col3:
            if source["processed"]:
                stats = source["stats"]
                st.markdown(f"Merged: {stats[3]}, New: {stats[4]}")

    # Process mode selection
    st.markdown("---")
    st.subheader("Processing Options")

    mode = st.radio(
        "Processing Mode",
        ["Auto-process all", "Review each decision"],
        help="Auto-process runs automatically. Review mode pauses for each merge/new decision."
    )

    # Source selection for processing
    unprocessed = [s for s in available_sources if not s["processed"]]

    if unprocessed:
        selected_sources = st.multiselect(
            "Select sources to process",
            [s["source_url"] for s in unprocessed],
            format_func=lambda x: next((s["title"][:50] for s in unprocessed if s["source_url"] == x), x)
        )

        if st.button("Process Selected Sources", type="primary", disabled=not selected_sources):
            pipeline = Phase3Pipeline()

            for source_url in selected_sources:
                st.markdown(f"### Processing: {source_url[:50]}...")

                # Load chunks for this source
                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT chunk_id, text, chunk_index
                            FROM sources.chunks
                            WHERE source_url = %s
                            ORDER BY chunk_index
                        """, (source_url,))
                        chunks = [
                            {"chunk_id": row[0], "text": row[1], "chunk_index": row[2]}
                            for row in cur.fetchall()
                        ]
                finally:
                    release_connection(conn)

                if chunks:
                    with st.spinner(f"Processing {len(chunks)} chunks..."):
                        try:
                            result = pipeline.process_source(
                                person_id=person.person_id,
                                chunks=chunks,
                                source_url=source_url,
                                review_mode=False,  # TODO: Implement review mode UI
                                save_checkpoints=True
                            )

                            st.success(f"Processed! Merged: {result['events_merged']}, Created: {result['events_created']}")

                            # Show decisions
                            with st.expander("Decision Log"):
                                for decision in result["decisions"][:10]:
                                    action = decision.get("action", "unknown")
                                    reasoning = decision.get("reasoning", "")
                                    st.markdown(f"- **{action}**: {reasoning[:100]}")

                        except Exception as e:
                            st.error(f"Error: {e}")
                else:
                    st.warning("No chunks found for this source")

            st.rerun()
    else:
        st.info("All available sources have been processed.")

else:
    st.info("No additional sources found in database for this person.")

# Finalize section
st.markdown("---")
if st.button("Finalize Phase 3", use_container_width=True):
    pipeline = Phase3Pipeline()
    result = pipeline.finalize(person.person_id)
    st.success(f"Phase 3 finalized! Total events: {result['total_events']}")
    st.balloons()
