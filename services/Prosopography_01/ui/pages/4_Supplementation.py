"""Supplementation page for Phase 3 source enrichment."""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, EventRepository, get_connection, release_connection
from phase3.pipeline import Phase3Pipeline
from source_search import SourceSearcher, chunk_text

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
            SELECT DISTINCT sr.url, sr.title, COUNT(c.id) as chunk_count
            FROM sources.search_results sr
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            JOIN sources.chunks c ON c.search_result_id = sr.id
            WHERE ps.person_name = %s
            AND sr.fetch_status = 'success'
            GROUP BY sr.url, sr.title
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
    pass  # Silently handle - sources schema may not have data for this person

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
                            SELECT c.id, c.text, c.chunk_index
                            FROM sources.chunks c
                            JOIN sources.search_results sr ON c.search_result_id = sr.id
                            WHERE sr.url = %s
                            ORDER BY c.chunk_index
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

# Search for new sources section
st.markdown("---")
st.subheader("Search for New Sources")

with st.expander("Search the web for additional sources", expanded=not available_sources):
    st.markdown("Search Google for sources about this person using Serper API.")

    col1, col2 = st.columns([3, 1])
    with col1:
        additional_terms = st.text_input(
            "Additional search terms (optional)",
            placeholder="e.g., ambassador, foreign minister"
        )
    with col2:
        num_results = st.selectbox("Results to fetch", [3, 5, 10], index=1)

    if st.button("Search & Fetch Sources", type="primary"):
        try:
            searcher = SourceSearcher()

            with st.spinner("Searching for sources..."):
                terms = [t.strip() for t in additional_terms.split(",")] if additional_terms else None
                sources = searcher.search_and_fetch(
                    person_name=selected_person,
                    additional_terms=terms,
                    num_results=num_results * 2,  # Search more, fetch top N
                    max_fetch=num_results,
                    exclude_wikipedia=True
                )

            if sources:
                st.success(f"Found {len(sources)} sources")

                # Store in session state for processing
                st.session_state.fetched_sources = sources

                for i, source in enumerate(sources):
                    status = "✅" if source["success"] else "❌"
                    title = source.get("search_title") or source.get("title") or source["url"][:50]
                    text_len = len(source.get("text", ""))

                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"{status} **{title[:60]}**")
                        st.caption(source["url"][:80])
                    with col2:
                        if source["success"]:
                            st.markdown(f"{text_len:,} chars")
                        else:
                            st.markdown(f"❌ {source.get('error', 'Failed')[:20]}")
            else:
                st.warning("No sources found. Try different search terms.")

        except ValueError as e:
            st.error(f"Configuration error: {e}")
        except Exception as e:
            st.error(f"Search error: {e}")

    # Process fetched sources
    if "fetched_sources" in st.session_state and st.session_state.fetched_sources:
        st.markdown("---")
        st.markdown("**Process Fetched Sources:**")

        successful_sources = [s for s in st.session_state.fetched_sources if s["success"]]

        if successful_sources:
            source_options = {
                s["url"]: f"{s.get('title', s['url'][:40])} ({len(s['text']):,} chars)"
                for s in successful_sources
            }

            selected_urls = st.multiselect(
                "Select sources to process",
                options=list(source_options.keys()),
                format_func=lambda x: source_options[x],
                default=list(source_options.keys())[:3]
            )

            if st.button("Process Selected Sources", key="process_fetched"):
                pipeline = Phase3Pipeline()

                for url in selected_urls:
                    source = next(s for s in successful_sources if s["url"] == url)
                    st.markdown(f"### Processing: {source.get('title', url)[:50]}...")

                    # Chunk the text
                    text = source["text"]
                    chunks = [
                        {"chunk_id": None, "text": chunk, "chunk_index": i}
                        for i, chunk in enumerate(chunk_text(text, chunk_size=4000))
                    ]

                    with st.spinner(f"Processing {len(chunks)} chunks..."):
                        try:
                            result = pipeline.process_source(
                                person_id=person.person_id,
                                chunks=chunks,
                                source_url=url,
                                review_mode=False,
                                save_checkpoints=True
                            )

                            st.success(f"Merged: {result['events_merged']}, Created: {result['events_created']}")

                        except Exception as e:
                            st.error(f"Error: {e}")

                # Clear fetched sources after processing
                del st.session_state.fetched_sources
                st.rerun()

# Manual source entry section
st.markdown("---")
st.subheader("Add Source Manually")

with st.expander("Paste source text manually"):
    manual_url = st.text_input("Source URL", placeholder="https://example.com/article")
    manual_text = st.text_area(
        "Source Text",
        height=200,
        placeholder="Paste the article text here..."
    )

    if st.button("Process Manual Source", disabled=not (manual_url and manual_text)):
        if manual_url and manual_text:
            pipeline = Phase3Pipeline()

            # Create a single chunk from the manual text
            chunks = [{"chunk_id": None, "text": manual_text, "chunk_index": 0}]

            with st.spinner("Processing manual source..."):
                try:
                    result = pipeline.process_source(
                        person_id=person.person_id,
                        chunks=chunks,
                        source_url=manual_url,
                        review_mode=False,
                        save_checkpoints=True
                    )

                    st.success(f"Processed! Merged: {result['events_merged']}, Created: {result['events_created']}")

                    with st.expander("Decision Log"):
                        for decision in result.get("decisions", [])[:10]:
                            action = decision.get("action", "unknown")
                            reasoning = decision.get("reasoning", "")
                            st.markdown(f"- **{action}**: {reasoning[:100]}")

                except Exception as e:
                    st.error(f"Error: {e}")
                    import traceback
                    st.code(traceback.format_exc())

# Finalize section
st.markdown("---")
if st.button("Finalize Phase 3", use_container_width=True):
    pipeline = Phase3Pipeline()
    result = pipeline.finalize(person.person_id)
    st.success(f"Phase 3 finalized! Total events: {result['total_events']}")
    st.balloons()
