"""Evidence panel component for displaying supporting quotes."""

import streamlit as st
from typing import List, Any


def render_evidence_panel(evidence_list: List[Any], max_expanded: int = 3) -> None:
    """Render a panel showing supporting evidence quotes.

    Args:
        evidence_list: List of SourceEvidence objects or dictionaries
        max_expanded: Number of quotes to show expanded by default
    """
    st.markdown("### Supporting Evidence")

    if not evidence_list:
        st.info("No supporting evidence found.")
        return

    st.markdown(f"**{len(evidence_list)} source(s):**")

    for i, evidence in enumerate(evidence_list):
        # Handle both objects and dictionaries
        if hasattr(evidence, "verbatim_quote"):
            quote = evidence.verbatim_quote
            source_url = evidence.source_url
            source_type = evidence.source_type
            evidence_type = evidence.evidence_type
            contribution = evidence.contribution
        else:
            quote = evidence.get("verbatim_quote", evidence.get("quote", ""))
            source_url = evidence.get("source_url", "")
            source_type = evidence.get("source_type", "")
            evidence_type = evidence.get("evidence_type", "original")
            contribution = evidence.get("contribution", "")

        # Format header
        type_badge = ""
        if evidence_type == "original":
            type_badge = "🔵 Original"
        elif evidence_type == "validation":
            type_badge = "✅ Validation"
        elif evidence_type == "supplementation":
            type_badge = "➕ Supplementation"

        header = f"Quote {i + 1} ({source_type}) {type_badge}"

        with st.expander(header, expanded=(i < max_expanded)):
            st.markdown(f"*{quote}*")
            if source_url:
                st.caption(f"Source: {source_url}")
            if contribution:
                st.caption(f"Contributes: {contribution}")


def render_evidence_summary(evidence_list: List[Any]) -> None:
    """Render a compact summary of evidence sources.

    Args:
        evidence_list: List of evidence items
    """
    if not evidence_list:
        st.markdown("**Sources:** None")
        return

    # Count by type
    type_counts = {}
    for evidence in evidence_list:
        if hasattr(evidence, "evidence_type"):
            et = evidence.evidence_type
        else:
            et = evidence.get("evidence_type", "original")
        type_counts[et] = type_counts.get(et, 0) + 1

    # Count unique sources
    unique_sources = set()
    for evidence in evidence_list:
        if hasattr(evidence, "source_url"):
            unique_sources.add(evidence.source_url)
        else:
            unique_sources.add(evidence.get("source_url", ""))

    parts = []
    if type_counts.get("original", 0):
        parts.append(f"{type_counts['original']} original")
    if type_counts.get("validation", 0):
        parts.append(f"{type_counts['validation']} validation")
    if type_counts.get("supplementation", 0):
        parts.append(f"{type_counts['supplementation']} supplementation")

    st.markdown(f"**Sources:** {len(unique_sources)} unique ({', '.join(parts)})")
