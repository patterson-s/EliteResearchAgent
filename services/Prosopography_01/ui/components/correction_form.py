"""Correction form component for capturing user corrections."""

import streamlit as st
from typing import Dict, Any, Optional, Callable


def render_correction_form(
    event_code: str,
    on_save: Optional[Callable[[Dict[str, Any]], None]] = None,
    on_validate: Optional[Callable[[], None]] = None,
    on_reject: Optional[Callable[[str], None]] = None
) -> Dict[str, Any]:
    """Render a correction form with action buttons.

    Args:
        event_code: Code of the event being corrected
        on_save: Callback when save is clicked
        on_validate: Callback when validate is clicked
        on_reject: Callback when reject is clicked

    Returns:
        Dictionary with form state
    """
    st.markdown("---")
    st.markdown("### Actions")

    col1, col2, col3, col4 = st.columns(4)

    form_state = {"action": None}

    with col1:
        if st.button("✅ Validate", key=f"validate_{event_code}", use_container_width=True):
            form_state["action"] = "validate"
            if on_validate:
                on_validate()

    with col2:
        if st.button("💾 Save Corrections", key=f"save_{event_code}", use_container_width=True, type="primary"):
            form_state["action"] = "save"
            if on_save:
                # Collect all corrections from session state
                corrections = collect_corrections_from_state(event_code)
                on_save(corrections)

    with col3:
        if st.button("❌ Reject Event", key=f"reject_{event_code}", use_container_width=True):
            form_state["action"] = "reject"

    with col4:
        if st.button("🗑️ Delete", key=f"delete_{event_code}", use_container_width=True):
            form_state["action"] = "delete"

    # Show rejection reason input if reject was clicked
    if form_state["action"] == "reject":
        reason = st.text_area(
            "Reason for rejection:",
            key=f"reject_reason_{event_code}",
            placeholder="Explain why this event should be rejected..."
        )
        if st.button("Confirm Rejection", key=f"confirm_reject_{event_code}"):
            if on_reject and reason:
                on_reject(reason)
            elif not reason:
                st.error("Please provide a reason for rejection.")

    return form_state


def collect_corrections_from_state(event_code: str) -> Dict[str, Any]:
    """Collect correction values from session state.

    Args:
        event_code: Code of the event

    Returns:
        Dictionary with corrections
    """
    corrections = {}

    # Check each field
    fields = [
        ("org", "organization"),
        ("time", "time_text"),
        ("roles", "roles"),
        ("locations", "locations")
    ]

    for field_key, field_name in fields:
        valid_key = f"{field_key}_valid_{event_code}"
        correction_key = f"{field_key}_correction_{event_code}"

        is_valid = st.session_state.get(valid_key, True)
        corrected_value = st.session_state.get(correction_key, "")

        if not is_valid or corrected_value:
            corrections[field_name] = {
                "is_valid": is_valid,
                "corrected_value": corrected_value if not is_valid else None
            }

    return corrections


def render_add_event_form(person_id: int, on_add: Optional[Callable[[Dict], None]] = None) -> None:
    """Render a form for adding a new event manually.

    Args:
        person_id: ID of the person
        on_add: Callback when add is clicked
    """
    st.markdown("### Add New Event")

    with st.form("add_event_form"):
        event_type = st.selectbox(
            "Event Type",
            ["career_position", "award"],
            format_func=lambda x: "Career Position" if x == "career_position" else "Award"
        )

        organization = st.text_input("Organization Name")
        roles = st.text_input("Roles (comma-separated)")
        locations = st.text_input("Locations (comma-separated)")

        col1, col2 = st.columns(2)
        with col1:
            time_start = st.text_input("Start Year", placeholder="YYYY")
        with col2:
            time_end = st.text_input("End Year", placeholder="YYYY or 'present'")

        time_text = st.text_input("Time Text (original)", placeholder="e.g., 'from 1998 to 2003'")
        supporting_quote = st.text_area("Supporting Evidence (optional)")
        source_url = st.text_input("Source URL (optional)")

        submitted = st.form_submit_button("Add Event", use_container_width=True)

        if submitted:
            if not roles:
                st.error("Roles are required")
            else:
                event_data = {
                    "person_id": person_id,
                    "event_type": event_type,
                    "organization_name": organization,
                    "roles": [r.strip() for r in roles.split(",")],
                    "locations": [l.strip() for l in locations.split(",")] if locations else [],
                    "time_start": time_start or None,
                    "time_end": time_end or None,
                    "time_text": time_text or None,
                    "supporting_quote": supporting_quote or None,
                    "source_url": source_url or None
                }
                if on_add:
                    on_add(event_data)
                st.success("Event added!")
                st.rerun()
