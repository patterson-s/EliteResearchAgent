"""Event card component for displaying career events."""

import streamlit as st
from typing import Dict, Any, Optional


def render_event_card(
    event: Any,
    event_idx: int,
    issues: list = None,
    show_validation_controls: bool = True
) -> Dict[str, Any]:
    """Render an event card with details and optional validation controls.

    Args:
        event: CareerEvent object or dictionary
        event_idx: Index of the event (for display)
        issues: List of verification issues for this event
        show_validation_controls: Whether to show validation checkboxes

    Returns:
        Dictionary with validation state from the form
    """
    validation_state = {}

    # Handle both CareerEvent objects and dictionaries
    if hasattr(event, "event_code"):
        event_code = event.event_code
        event_type = event.event_type
        org_name = event.org_name or "Unknown"
        time_text = event.time_text or f"{event.time_start or '?'} - {event.time_end or '?'}"
        roles = event.roles or []
        locations = event.locations or []
        confidence = event.confidence
        validation_status = event.validation_status
        event_id = event.event_id
    else:
        event_code = event.get("event_code", event.get("event_id", ""))
        event_type = event.get("event_type", "career_position")
        org_name = event.get("org_name", event.get("canonical_org_name", "Unknown"))
        time_period = event.get("time_period", {})
        time_text = time_period.get("text") or f"{time_period.get('start', '?')} - {time_period.get('end', '?')}"
        roles = event.get("roles", [])
        locations = event.get("locations", [])
        confidence = event.get("confidence", "medium")
        validation_status = event.get("validation_status", "pending")
        event_id = event.get("event_id")

    # Header
    st.markdown(f"### Event {event_idx + 1}: {event_code}")

    type_label = "CAREER POSITION" if event_type == "career_position" else "AWARD"
    status_color = {
        "validated": "green",
        "rejected": "red",
        "pending": "orange",
        "needs_review": "blue"
    }.get(validation_status, "gray")

    st.markdown(f"""
    **Type:** {type_label} | **Status:** :{status_color}[{validation_status}] | **Confidence:** {confidence}
    """)

    # Show issues if any
    if issues:
        for issue in issues:
            severity = issue.severity if hasattr(issue, "severity") else issue.get("severity", "warning")
            desc = issue.description if hasattr(issue, "description") else issue.get("description", "")
            if severity == "error":
                st.error(f"**{issue.issue_type if hasattr(issue, 'issue_type') else issue.get('issue_type', '')}**: {desc}")
            else:
                st.warning(f"**{issue.issue_type if hasattr(issue, 'issue_type') else issue.get('issue_type', '')}**: {desc}")

    st.markdown("---")

    # Organization
    if show_validation_controls:
        col1, col2 = st.columns([1, 3])
        with col1:
            org_valid = st.checkbox("Org correct", value=True, key=f"org_valid_{event_code}")
            validation_state["org_valid"] = org_valid
        with col2:
            st.markdown(f"**Organization:** {org_name}")
            if not org_valid:
                validation_state["org_correction"] = st.text_input(
                    "Correct organization",
                    key=f"org_correction_{event_code}",
                    placeholder="Enter correct name"
                )
    else:
        st.markdown(f"**Organization:** {org_name}")

    # Time Period
    if show_validation_controls:
        col1, col2 = st.columns([1, 3])
        with col1:
            time_valid = st.checkbox("Time correct", value=True, key=f"time_valid_{event_code}")
            validation_state["time_valid"] = time_valid
        with col2:
            st.markdown(f"**Time Period:** {time_text}")
            if not time_valid:
                validation_state["time_correction"] = st.text_input(
                    "Correct time period",
                    key=f"time_correction_{event_code}",
                    placeholder="YYYY-YYYY"
                )
    else:
        st.markdown(f"**Time Period:** {time_text}")

    # Roles
    if roles:
        if show_validation_controls:
            col1, col2 = st.columns([1, 3])
            with col1:
                roles_valid = st.checkbox("Roles correct", value=True, key=f"roles_valid_{event_code}")
                validation_state["roles_valid"] = roles_valid
            with col2:
                st.markdown(f"**Roles:** {', '.join(roles)}")
                if not roles_valid:
                    validation_state["roles_correction"] = st.text_input(
                        "Correct roles",
                        key=f"roles_correction_{event_code}",
                        placeholder="Comma-separated roles"
                    )
        else:
            st.markdown(f"**Roles:** {', '.join(roles)}")

    # Locations
    if locations:
        if show_validation_controls:
            col1, col2 = st.columns([1, 3])
            with col1:
                locations_valid = st.checkbox("Locations correct", value=True, key=f"locations_valid_{event_code}")
                validation_state["locations_valid"] = locations_valid
            with col2:
                st.markdown(f"**Locations:** {', '.join(locations)}")
                if not locations_valid:
                    validation_state["locations_correction"] = st.text_input(
                        "Correct locations",
                        key=f"locations_correction_{event_code}",
                        placeholder="Comma-separated locations"
                    )
        else:
            st.markdown(f"**Locations:** {', '.join(locations)}")

    return validation_state
