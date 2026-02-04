"""Evaluation page for viewing metrics and quality assessment."""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, IssueRepository
from evaluation.metrics import MetricsCalculator
from validation.issue_tracker import IssueTracker

st.set_page_config(page_title="Evaluation", layout="wide")
st.title("Evaluation & Metrics")

# Initialize
person_repo = PersonRepository()
metrics_calc = MetricsCalculator()
issue_tracker = IssueTracker()
issue_repo = IssueRepository()

# Person selection
persons = person_repo.get_all()

if not persons:
    st.info("No persons in database.")
    st.stop()

person_names = ["All Persons"] + [p.person_name for p in persons]
selected = st.selectbox("Select Person", person_names)

if selected == "All Persons":
    # Dashboard summary
    st.subheader("Overall Summary")

    try:
        summary = metrics_calc.get_dashboard_summary()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Persons", summary["total_persons"])
        with col2:
            st.metric("Total Events", summary["total_events"])
        with col3:
            st.metric("Validated Events", summary["total_validated"])
        with col4:
            st.metric("Open Issues", summary["total_open_issues"])

        # Status breakdown
        st.markdown("---")
        st.subheader("Persons by Status")

        status_counts = summary["persons_by_status"]
        for status, count in status_counts.items():
            st.markdown(f"- **{status}**: {count}")

    except Exception as e:
        st.error(f"Error loading summary: {e}")

    # Global issues
    st.markdown("---")
    st.subheader("Open Issues")

    open_issues = issue_repo.get_all_open()
    if open_issues:
        for issue in open_issues[:20]:
            severity = issue["severity"]
            if severity == "error":
                st.error(f"**{issue['person_name']}** - {issue['event_code']}: {issue['description']}")
            else:
                st.warning(f"**{issue['person_name']}** - {issue['event_code']}: {issue['description']}")
    else:
        st.success("No open issues!")

else:
    # Individual person metrics
    person = person_repo.get_by_name(selected)
    if not person:
        st.stop()

    try:
        metrics = metrics_calc.calculate_all_metrics(person.person_id)

        # Header
        st.markdown(f"**Status:** {metrics['workflow_status']}")

        # Extraction Quality
        st.markdown("---")
        st.subheader("Extraction Quality")

        eq = metrics["extraction_quality"]
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Events", eq["total_events"])
        with col2:
            st.metric("Validated", f"{eq['validated']} ({eq['validation_rate']:.0%})")
        with col3:
            st.metric("Rejected", f"{eq['rejected']} ({eq['rejection_rate']:.0%})")
        with col4:
            st.metric("Corrected", f"{eq['events_corrected']} ({eq['correction_rate']:.0%})")

        # Field Accuracy
        st.markdown("---")
        st.subheader("Field-Level Accuracy")

        fa = metrics["field_accuracy"]["field_accuracy"]

        cols = st.columns(len(fa))
        for i, (field, accuracy) in enumerate(fa.items()):
            with cols[i]:
                st.metric(field.replace("_", " ").title(), f"{accuracy:.0%}")

        # Source Coverage
        st.markdown("---")
        st.subheader("Source Coverage")

        sc = metrics["source_coverage"]
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Sources Processed", sc["sources_processed"])
        with col2:
            st.metric("Multi-Source Events", sc["multi_source_events"])
        with col3:
            st.metric("Multi-Source Rate", f"{sc['multi_source_rate']:.0%}")

        if sc["sources_by_type"]:
            st.markdown("**Sources by Type:**")
            for stype, count in sc["sources_by_type"].items():
                st.markdown(f"- {stype}: {count}")

        # Issues
        st.markdown("---")
        st.subheader("Issues")

        iss = metrics["issues"]
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Issues", iss["total_issues"])
        with col2:
            st.metric("Open", iss["open_issues"])
        with col3:
            st.metric("Resolved", iss["resolved_issues"])
        with col4:
            st.metric("Resolution Rate", f"{iss['resolution_rate']:.0%}")

        # Open issues list
        if iss["open_errors"] > 0 or iss["open_warnings"] > 0:
            st.markdown("**Open Issues:**")
            issues = issue_repo.get_for_person(person.person_id, include_resolved=False)
            for issue in issues:
                if issue.severity == "error":
                    st.error(f"**{issue.issue_type}**: {issue.description}")
                else:
                    st.warning(f"**{issue.issue_type}**: {issue.description}")

        # Correction breakdown
        st.markdown("---")
        st.subheader("Correction Breakdown")

        corrections_by_field = metrics["field_accuracy"]["corrections_by_field"]
        if corrections_by_field:
            for field, data in corrections_by_field.items():
                total = data.get("total", 0)
                invalid = data.get("invalid", 0)
                st.markdown(f"- **{field}**: {invalid} corrections out of {total} reviews")
        else:
            st.info("No corrections recorded.")

    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        import traceback
        st.code(traceback.format_exc())
