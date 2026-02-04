"""Review Events page for Phase 2 validation and correction."""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import PersonRepository, EventRepository, EvidenceRepository, IssueRepository
from phase2.correction_service import CorrectionService
from phase2.event_editor import EventEditor
from ui.components.event_card import render_event_card
from ui.components.evidence_panel import render_evidence_panel
from ui.components.correction_form import render_add_event_form

st.set_page_config(page_title="Review Events", layout="wide")
st.title("Review Events (Phase 2)")

# Initialize
person_repo = PersonRepository()
event_repo = EventRepository()
evidence_repo = EvidenceRepository()
issue_repo = IssueRepository()
correction_service = CorrectionService()
event_editor = EventEditor()

# Person selection
persons = person_repo.get_all()
eligible = [p for p in persons if p.workflow_status in ["phase1_complete", "phase2_reviewed", "phase3_in_progress", "complete"]]

if not eligible:
    st.warning("No persons ready for review. Run Phase 1 first from Template Builder.")
    st.stop()

person_names = [p.person_name for p in eligible]
selected_person = st.selectbox("Select Person", person_names)
person = person_repo.get_by_name(selected_person)

if not person:
    st.stop()

# Get events
events = event_repo.get_for_person(person.person_id)
events = [e for e in events if e.validation_status != "rejected"]

if not events:
    st.info("No events found for this person.")
    st.stop()

# Sidebar navigation
st.sidebar.title("Navigation")
st.sidebar.markdown(f"**{len(events)} events**")

# Event index tracking
if "event_idx" not in st.session_state:
    st.session_state.event_idx = 0

event_idx = st.session_state.event_idx
if event_idx >= len(events):
    event_idx = 0
    st.session_state.event_idx = 0

# Jump to event
event_options = [f"{i+1}. {e.event_code}" for i, e in enumerate(events)]
selected_idx = st.sidebar.selectbox(
    "Jump to event",
    range(len(events)),
    index=event_idx,
    format_func=lambda i: event_options[i]
)
if selected_idx != event_idx:
    st.session_state.event_idx = selected_idx
    st.rerun()

# Progress
progress = (event_idx + 1) / len(events)
st.sidebar.progress(progress, text=f"{event_idx + 1}/{len(events)}")

# Count validated
validated_count = sum(1 for e in events if e.validation_status == "validated")
st.sidebar.metric("Validated", f"{validated_count}/{len(events)}")

# Current event
current_event = events[event_idx]

# Get issues for this event
issues = issue_repo.get_for_event(current_event.event_id, include_resolved=False)

# Get evidence
evidence = evidence_repo.get_for_event(current_event.event_id)

# Main content - two columns
col1, col2 = st.columns([1, 1])

with col1:
    validation_state = render_event_card(
        current_event,
        event_idx,
        issues=issues,
        show_validation_controls=True
    )

    # Action buttons
    st.markdown("---")
    action_col1, action_col2, action_col3, action_col4 = st.columns(4)

    with action_col1:
        if st.button("✅ Validate", use_container_width=True):
            correction_service.validate_event(current_event.event_id)
            st.success("Event validated!")
            st.rerun()

    with action_col2:
        if st.button("💾 Save", type="primary", use_container_width=True):
            # Collect and apply corrections
            for field_key, field_name in [("org", "organization"), ("time", "time_text"), ("roles", "roles"), ("locations", "locations")]:
                is_valid = st.session_state.get(f"{field_key}_valid_{current_event.event_code}", True)
                correction = st.session_state.get(f"{field_key}_correction_{current_event.event_code}", "")

                if not is_valid or correction:
                    correction_service.apply_correction(
                        event_id=current_event.event_id,
                        field_name=field_name,
                        is_valid=is_valid,
                        corrected_value=correction if not is_valid else None
                    )

            st.success("Corrections saved!")
            st.rerun()

    with action_col3:
        if st.button("❌ Reject", use_container_width=True):
            st.session_state.show_reject_dialog = True

    with action_col4:
        if st.button("🗑️ Delete", use_container_width=True):
            event_editor.delete_event(current_event.event_id, "Deleted by user")
            st.success("Event deleted")
            st.rerun()

    # Reject dialog
    if st.session_state.get("show_reject_dialog"):
        reason = st.text_area("Reason for rejection:")
        if st.button("Confirm Rejection"):
            if reason:
                correction_service.reject_event(current_event.event_id, reason)
                st.session_state.show_reject_dialog = False
                st.success("Event rejected")
                st.rerun()
            else:
                st.error("Please provide a reason")

with col2:
    render_evidence_panel(evidence, person_name=selected_person)

# Navigation buttons at bottom
st.markdown("---")
nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])

with nav_col1:
    if st.button("⬅️ Previous", disabled=(event_idx == 0), use_container_width=True):
        st.session_state.event_idx = event_idx - 1
        st.rerun()

with nav_col2:
    if st.button("Finalize Phase 2", type="secondary", use_container_width=True):
        result = correction_service.finalize_review(person.person_id)
        st.success(f"Phase 2 finalized! {result['status_counts']['validated']} events validated.")

with nav_col3:
    if st.button("Next ➡️", disabled=(event_idx == len(events) - 1), use_container_width=True):
        st.session_state.event_idx = event_idx + 1
        st.rerun()

# Add missing event section
st.markdown("---")
with st.expander("Add Missing Event"):
    render_add_event_form(
        person.person_id,
        on_add=lambda data: event_editor.add_event(**data)
    )
