"""Dashboard page showing all persons and their status."""

import streamlit as st
import sys
from pathlib import Path
import shutil
import time
import os

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, reset_pool
from evaluation.metrics import MetricsCalculator
from batch_processor import BatchProcessor, ProcessingStatus

st.set_page_config(page_title="Dashboard", layout="wide")
st.title("Dashboard")

# Initialize batch processor in session state
if "batch_processor" not in st.session_state:
    st.session_state.batch_processor = None
if "batch_running" not in st.session_state:
    st.session_state.batch_running = False
if "batch_results" not in st.session_state:
    st.session_state.batch_results = None

# Sidebar controls
with st.sidebar:
    st.subheader("Database Controls")
    if st.button("🔄 Reset Connection Pool", help="Use if you see 'connection pool exhausted' errors"):
        reset_pool()
        st.success("Connection pool reset!")
        st.rerun()

    st.markdown("---")
    st.subheader("Batch Processing")

    # Check for Serper API key
    has_serper = bool(os.getenv("SERPER_API_KEY"))

    if not has_serper:
        st.warning("SERPER_API_KEY not set. Batch processing requires Serper for Wikipedia fetching.")
    else:
        # Get unprocessed persons count
        try:
            temp_repo = PersonRepository()
            summaries = temp_repo.get_summary()
            unprocessed = [
                s for s in summaries
                if s.get("event_count", 0) == 0 and s.get("validated_count", 0) == 0
            ]
            unprocessed_count = len(unprocessed)
        except Exception:
            unprocessed_count = 0
            unprocessed = []

        st.markdown(f"**{unprocessed_count}** people with 0 events")

        if st.session_state.batch_running:
            # Show progress while running
            if st.button("⏹️ Stop Batch", type="secondary", use_container_width=True):
                if st.session_state.batch_processor:
                    st.session_state.batch_processor.stop()
                st.session_state.batch_running = False
                st.rerun()
        else:
            # Start button
            if st.button(
                "🚀 Run Batch Phase 1",
                type="primary",
                use_container_width=True,
                disabled=(unprocessed_count == 0),
                help=f"Process {unprocessed_count} people through Phase 1"
            ):
                st.session_state.batch_running = True
                st.session_state.batch_results = None
                st.rerun()

        # Show results if we have them
        if st.session_state.batch_results:
            results = st.session_state.batch_results
            st.markdown("---")
            st.markdown("**Last Batch Results:**")
            st.markdown(f"✅ Complete: {results.completed}")
            st.markdown(f"❌ Failed: {results.failed}")
            st.markdown(f"⏭️ Skipped: {results.skipped}")

# Initialize repositories
person_repo = PersonRepository()
metrics_calc = MetricsCalculator()

# Run batch processing if triggered
if st.session_state.batch_running:
    st.markdown("---")
    st.subheader("🔄 Batch Processing in Progress")

    # Get unprocessed persons
    summaries = person_repo.get_summary()
    unprocessed = [
        {"person_id": s["person_id"], "person_name": s["person_name"]}
        for s in summaries
        if s.get("event_count", 0) == 0 and s.get("validated_count", 0) == 0
    ]

    if not unprocessed:
        st.info("No unprocessed persons found.")
        st.session_state.batch_running = False
        st.rerun()
    else:
        # Create progress display
        progress_bar = st.progress(0, text="Starting batch processing...")
        status_container = st.container()

        # Initialize processor
        processor = BatchProcessor(max_workers=5)
        st.session_state.batch_processor = processor

        # Create a placeholder for live updates
        with status_container:
            results_placeholder = st.empty()

            # Run batch processing
            def update_display(progress):
                pct = progress.get_progress_pct()
                done = progress.completed + progress.failed + progress.skipped
                progress_bar.progress(pct, text=f"Processing: {done}/{progress.total} complete")

                # Build status display
                status_lines = []
                for pid, result in sorted(progress.results.items(), key=lambda x: x[1].person_name):
                    if result.status == ProcessingStatus.PENDING:
                        status_lines.append(f"⏳ {result.person_name}")
                    elif result.status == ProcessingStatus.FETCHING_WIKIPEDIA:
                        status_lines.append(f"🔍 {result.person_name} - Fetching Wikipedia...")
                    elif result.status == ProcessingStatus.RUNNING_PIPELINE:
                        status_lines.append(f"⚙️ {result.person_name} - Running Phase 1...")
                    elif result.status == ProcessingStatus.COMPLETE:
                        status_lines.append(f"✅ {result.person_name} - {result.events_found} events")
                    elif result.status == ProcessingStatus.FAILED:
                        status_lines.append(f"❌ {result.person_name} - {result.error_message[:50]}")
                    elif result.status == ProcessingStatus.SKIPPED:
                        status_lines.append(f"⏭️ {result.person_name} - {result.error_message[:50]}")

                results_placeholder.markdown("\n\n".join(status_lines))

            # Run the batch
            final_progress = processor.run_batch(unprocessed, progress_callback=update_display)

            # Store results and finish
            st.session_state.batch_results = final_progress
            st.session_state.batch_running = False
            st.session_state.batch_processor = None

            progress_bar.progress(1.0, text="Batch processing complete!")
            st.success(f"Batch complete! ✅ {final_progress.completed} | ❌ {final_progress.failed} | ⏭️ {final_progress.skipped}")

            time.sleep(2)
            st.rerun()

# Filters
col1, col2 = st.columns([3, 1])
with col1:
    search = st.text_input("Search by name", placeholder="Enter person name...")
with col2:
    status_filter = st.selectbox(
        "Filter by status",
        ["All", "pending", "phase1_complete", "phase2_reviewed", "phase3_in_progress", "complete"]
    )

# Get persons
try:
    if status_filter == "All":
        persons = person_repo.get_all()
    else:
        persons = person_repo.get_all(status_filter=status_filter)

    # Apply search filter
    if search:
        persons = [p for p in persons if search.lower() in p.person_name.lower()]

    # Get summary for each person
    summaries = person_repo.get_summary()
    summary_map = {s["person_id"]: s for s in summaries}

    # Display persons table
    st.markdown(f"**{len(persons)} persons**")

    if persons:
        # Create table data
        for person in persons:
            summary = summary_map.get(person.person_id, {})

            with st.container():
                col1, col2, col3, col4, col5, col6 = st.columns([3, 2, 1, 1, 1, 1])

                with col1:
                    st.markdown(f"**{person.person_name}**")

                with col2:
                    status = person.workflow_status
                    color = {
                        "pending": "orange",
                        "phase1_complete": "blue",
                        "phase2_reviewed": "violet",
                        "phase3_in_progress": "violet",
                        "complete": "green"
                    }.get(status, "gray")
                    st.markdown(f":{color}[{status}]")

                with col3:
                    st.metric("Events", summary.get("event_count", 0))

                with col4:
                    st.metric("Validated", summary.get("validated_count", 0))

                with col5:
                    errors = summary.get("open_errors", 0)
                    warnings = summary.get("open_warnings", 0)
                    if errors > 0:
                        st.error(f"{errors} errors")
                    elif warnings > 0:
                        st.warning(f"{warnings} warnings")
                    else:
                        st.success("No issues")

                with col6:
                    # Delete button with confirmation
                    delete_key = f"delete_{person.person_id}"
                    confirm_key = f"confirm_{person.person_id}"

                    if st.session_state.get(confirm_key):
                        col_yes, col_no = st.columns(2)
                        with col_yes:
                            if st.button("✓", key=f"yes_{person.person_id}", help="Confirm delete"):
                                # Delete from database
                                person_repo.delete(person.person_id)
                                # Also delete review folder if exists
                                safe_name = person.person_name.replace(" ", "_")
                                review_dir = Path(__file__).parent.parent.parent / "review" / safe_name
                                if review_dir.exists():
                                    shutil.rmtree(review_dir)
                                st.session_state[confirm_key] = False
                                st.rerun()
                        with col_no:
                            if st.button("✗", key=f"no_{person.person_id}", help="Cancel"):
                                st.session_state[confirm_key] = False
                                st.rerun()
                    else:
                        if st.button("🗑️", key=delete_key, help="Delete person and all data"):
                            st.session_state[confirm_key] = True
                            st.rerun()

                st.markdown("---")

    else:
        st.info("No persons found matching your criteria.")

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the database is connected and the schema is created.")

# Add new person section
st.markdown("---")
st.subheader("Add New Person")

with st.form("add_person"):
    new_name = st.text_input("Person Name")
    submitted = st.form_submit_button("Add Person")

    if submitted and new_name:
        try:
            from db import Person
            person = Person(person_name=new_name)
            person_id = person_repo.create(person)
            st.success(f"Created person: {new_name} (ID: {person_id})")
            st.rerun()
        except Exception as e:
            st.error(f"Error creating person: {e}")
