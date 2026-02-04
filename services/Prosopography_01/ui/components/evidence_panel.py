"""Evidence panel component for displaying supporting quotes."""

import streamlit as st
from typing import List, Any, Optional
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import get_connection, release_connection


def get_chunk_text(chunk_id: Optional[int]) -> Optional[str]:
    """Fetch the full chunk text from the database.

    Args:
        chunk_id: The chunk ID to fetch

    Returns:
        The chunk text or None if not found
    """
    if not chunk_id:
        return None

    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT text FROM sources.chunks WHERE id = %s",
                (chunk_id,)
            )
            row = cur.fetchone()
            release_connection(conn)
            return row[0] if row else None
    except Exception:
        return None


def highlight_quote_in_chunk(chunk_text: str, quote: str) -> str:
    """Highlight the quote within the chunk text using markdown.

    Args:
        chunk_text: The full chunk text
        quote: The quote to highlight

    Returns:
        HTML/markdown with the quote highlighted
    """
    if not chunk_text or not quote:
        return chunk_text or ""

    # Try exact match first
    if quote in chunk_text:
        # Split around the quote and add highlighting
        parts = chunk_text.split(quote, 1)
        return f"{parts[0]}**🔍 {quote}**{parts[1] if len(parts) > 1 else ''}"

    # Try case-insensitive match
    lower_chunk = chunk_text.lower()
    lower_quote = quote.lower()
    if lower_quote in lower_chunk:
        idx = lower_chunk.find(lower_quote)
        original_quote = chunk_text[idx:idx + len(quote)]
        parts = chunk_text.split(original_quote, 1)
        return f"{parts[0]}**🔍 {original_quote}**{parts[1] if len(parts) > 1 else ''}"

    # Quote not found in chunk - just return chunk
    return chunk_text


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
            chunk_id = evidence.chunk_id
        else:
            quote = evidence.get("verbatim_quote", evidence.get("quote", ""))
            source_url = evidence.get("source_url", "")
            source_type = evidence.get("source_type", "")
            evidence_type = evidence.get("evidence_type", "original")
            contribution = evidence.get("contribution", "")
            chunk_id = evidence.get("chunk_id")

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
            # Show the quote
            st.markdown(f"**Quote:** *{quote}*")

            # Try to get and show full chunk context with quote highlighted
            chunk_text = get_chunk_text(chunk_id)
            if chunk_text:
                st.markdown("---")
                st.markdown("**Full Context (quote highlighted):**")
                highlighted_text = highlight_quote_in_chunk(chunk_text, quote)
                # Use a container with background for readability
                st.markdown(
                    f'<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; font-size: 0.9em;">{highlighted_text}</div>',
                    unsafe_allow_html=True
                )

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
