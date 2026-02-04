"""Dashboard page showing all persons and their status."""

import streamlit as st
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository
from evaluation.metrics import MetricsCalculator

st.set_page_config(page_title="Dashboard", layout="wide")
st.title("Dashboard")

# Initialize repositories
person_repo = PersonRepository()
metrics_calc = MetricsCalculator()

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
                col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1])

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
