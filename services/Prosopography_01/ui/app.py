"""Main Streamlit application entry point."""

import streamlit as st

st.set_page_config(
    page_title="Prosopography Tool",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Prosopography Tool")
st.markdown("""
Welcome to the Prosopography Tool - a system for extracting, validating, and enriching
career event data from biographical sources.

### Workflow

1. **Dashboard** - View all persons and their status
2. **Template Builder** - Extract initial career events from Wikipedia (Phase 1)
3. **Review Events** - Validate and correct the extracted events (Phase 2)
4. **Supplementation** - Enrich events with additional sources (Phase 3)
5. **Evaluation** - View metrics and quality assessment

### Getting Started

Select a page from the sidebar to begin.
""")

# Display quick stats if available
try:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from evaluation.metrics import MetricsCalculator

    calc = MetricsCalculator()
    summary = calc.get_dashboard_summary()

    st.markdown("---")
    st.subheader("Quick Stats")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Persons", summary["total_persons"])

    with col2:
        st.metric("Total Events", summary["total_events"])

    with col3:
        st.metric("Validated", summary["total_validated"])

    with col4:
        st.metric("Open Issues", summary["total_open_issues"])

except Exception as e:
    st.info("Connect to database to see statistics.")
