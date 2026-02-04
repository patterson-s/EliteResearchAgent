"""Template Builder page for Phase 1 extraction."""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, get_connection, release_connection
from phase1.pipeline import Phase1Pipeline

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
            SELECT c.chunk_id, c.text, c.chunk_index, c.source_url
            FROM sources.chunks c
            JOIN sources.search_results sr ON c.source_url = sr.url
            JOIN sources.persons_searched ps ON sr.person_id = ps.person_id
            WHERE ps.person_name = %s
            AND c.source_url LIKE '%%wikipedia%%'
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
    st.info(f"Could not load Wikipedia chunks from database: {e}")

if wikipedia_chunks:
    st.success(f"Found {len(wikipedia_chunks)} Wikipedia chunks in database")
    source_url = wikipedia_chunks[0]["source_url"]
    st.markdown(f"**Source:** {source_url}")

    # Show preview
    with st.expander("Preview Wikipedia text"):
        full_text = "\n".join(c["text"] for c in wikipedia_chunks)
        st.text_area("Content", full_text[:5000] + "..." if len(full_text) > 5000 else full_text, height=300)

else:
    st.info("No Wikipedia chunks found in database. You can paste Wikipedia text manually.")
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
