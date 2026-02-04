"""Template Builder page for Phase 1 extraction."""

import streamlit as st
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, get_connection, release_connection
from phase1.pipeline import Phase1Pipeline
from source_search import SourceSearcher, chunk_text

st.set_page_config(page_title="Template Builder", layout="wide")
st.title("Template Builder (Phase 1)")

st.markdown("""
Extract initial career events from Wikipedia. This is Phase 1 of the workflow.
""")

# Initialize
person_repo = PersonRepository()

# Person selection
persons = person_repo.get_all()
person_names = [p.person_name for p in persons]

if not person_names:
    st.warning("No persons in database. Add a person from the Dashboard first.")
    st.stop()

selected_person = st.selectbox("Select Person", person_names)
person = person_repo.get_by_name(selected_person)

if person:
    st.markdown(f"**Current Status:** {person.workflow_status}")

    if person.workflow_status not in ["pending"]:
        st.warning("Phase 1 has already been run for this person. Running again will overwrite existing data.")

# Source selection
st.markdown("---")
st.subheader("Wikipedia Source")

# Try to load Wikipedia chunks from database
wikipedia_chunks = []
try:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.text, c.chunk_index, sr.url
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.search_result_id = sr.id
            JOIN sources.persons_searched ps ON sr.person_search_id = ps.id
            WHERE ps.person_name = %s
            AND sr.url LIKE '%%wikipedia%%'
            ORDER BY c.chunk_index
        """, (selected_person,))
        rows = cur.fetchall()
        for row in rows:
            wikipedia_chunks.append({
                "chunk_id": row[0],
                "text": row[1],
                "chunk_index": row[2],
                "source_url": row[3]
            })
    release_connection(conn)
except Exception as e:
    pass  # Silently fall back to manual entry

if wikipedia_chunks:
    st.success(f"Found {len(wikipedia_chunks)} Wikipedia chunks in database")
    source_url = wikipedia_chunks[0]["source_url"]
    st.markdown(f"**Source:** {source_url}")

    # Show preview
    with st.expander("Preview Wikipedia text"):
        full_text = "\n".join(c["text"] for c in wikipedia_chunks)
        st.text_area("Content", full_text[:5000] + "..." if len(full_text) > 5000 else full_text, height=300)

else:
    st.info("No Wikipedia chunks found in database. Fetch from Wikipedia or paste text manually.")

    # Check if we have fetched Wikipedia content in session state
    if "wikipedia_content" in st.session_state and st.session_state.wikipedia_content:
        wiki_data = st.session_state.wikipedia_content
        st.success(f"✅ Wikipedia article fetched: {wiki_data['title']}")
        st.markdown(f"**Source:** [{wiki_data['url']}]({wiki_data['url']})")

        with st.expander("Preview Wikipedia text", expanded=False):
            preview_text = wiki_data['text'][:5000] + "..." if len(wiki_data['text']) > 5000 else wiki_data['text']
            st.text_area("Content", preview_text, height=300, disabled=True)

        st.markdown(f"**Total characters:** {len(wiki_data['text']):,}")
        source_url = wiki_data['url']
        wikipedia_chunks = [{"text": wiki_data['text'], "chunk_id": None, "source_url": source_url}]

        # Option to clear and try again
        if st.button("🔄 Clear and fetch different source"):
            del st.session_state.wikipedia_content
            st.rerun()

    else:
        # Option 1: Auto-fetch from Wikipedia
        st.markdown("#### Option 1: Fetch from Wikipedia")

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"Search for **{selected_person}** on Wikipedia")
        with col2:
            fetch_button = st.button("🔍 Fetch Wikipedia", type="primary")

        if fetch_button:
            # Check if Serper API key is available
            if not os.getenv("SERPER_API_KEY"):
                st.error("SERPER_API_KEY not configured. Please add it to environment variables or use manual paste below.")
            else:
                try:
                    searcher = SourceSearcher()

                    with st.spinner(f"Searching for {selected_person} on Wikipedia..."):
                        # Search specifically for Wikipedia
                        results = searcher.search(
                            f'"{selected_person}" site:wikipedia.org',
                            num_results=3
                        )

                        # Find the best Wikipedia result
                        wiki_url = None
                        for result in results:
                            url = result.get("url", "")
                            if "wikipedia.org/wiki/" in url and not any(x in url for x in ["/File:", "/Category:", "/Template:", "/Talk:"]):
                                wiki_url = url
                                break

                        if wiki_url:
                            st.info(f"Found: {wiki_url}")

                            # Fetch the content
                            with st.spinner("Fetching Wikipedia article..."):
                                content = searcher.fetch_content(wiki_url)

                            if content["success"]:
                                # Store in session state
                                st.session_state.wikipedia_content = {
                                    "url": wiki_url,
                                    "title": content.get("title", selected_person),
                                    "text": content["text"]
                                }
                                st.rerun()
                            else:
                                st.error(f"Failed to fetch Wikipedia: {content.get('error', 'Unknown error')}")
                        else:
                            st.warning(f"No Wikipedia article found for '{selected_person}'. Try manual paste below.")

                except ValueError as e:
                    st.error(f"Configuration error: {e}")
                except Exception as e:
                    st.error(f"Search error: {e}")

        st.markdown("---")

        # Option 2: Manual paste
        st.markdown("#### Option 2: Paste manually")
        manual_text = st.text_area(
            "Paste Wikipedia article text",
            height=300,
            placeholder="Paste the Wikipedia article content here..."
        )
        source_url = st.text_input("Wikipedia URL", placeholder="https://en.wikipedia.org/wiki/...")

        if manual_text:
            wikipedia_chunks = [{"text": manual_text, "chunk_id": None, "source_url": source_url}]

# Run pipeline
st.markdown("---")
if st.button("Run Phase 1 Pipeline", type="primary", disabled=not wikipedia_chunks):
    if not wikipedia_chunks:
        st.error("No Wikipedia content available")
    else:
        with st.spinner("Running Phase 1 pipeline..."):
            progress = st.progress(0)
            status = st.empty()

            try:
                pipeline = Phase1Pipeline()

                status.text("Step 1/4: Extracting entities...")
                progress.progress(10)

                # Run the pipeline
                if len(wikipedia_chunks) > 1:
                    result = pipeline.run_from_chunks(
                        person_name=selected_person,
                        chunks_data=wikipedia_chunks,
                        save_checkpoints=True
                    )
                else:
                    full_text = wikipedia_chunks[0]["text"]
                    result = pipeline.run(
                        person_name=selected_person,
                        wikipedia_text=full_text,
                        source_url=source_url or "manual_entry",
                        save_checkpoints=True
                    )

                progress.progress(100)
                status.text("Complete!")

                # Show results
                st.success("Phase 1 complete!")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Chunks Processed", result["steps"]["step1"]["total_chunks"])
                with col2:
                    st.metric("Events Assembled", result["steps"]["step3"]["events_count"])
                with col3:
                    summary = result["steps"]["step4"]
                    st.metric("Valid Events", summary.get("valid", 0))

                st.info("Proceed to 'Review Events' page to validate the extracted events.")

            except Exception as e:
                st.error(f"Pipeline error: {e}")
                import traceback
                st.code(traceback.format_exc())
