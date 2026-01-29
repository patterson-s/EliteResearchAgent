import json
import streamlit as st
from pathlib import Path
from typing import Dict, Any, List

st.set_page_config(page_title="Career Timeline Viewer", layout="wide")

@st.cache_data
def load_timeline(file_path: Path) -> Dict[str, Any]:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_display_values(event: Dict[str, Any]) -> Dict[str, Any]:
    org = event.get('organization')
    role = event.get('role')
    variants = event.get('variants', {})
    
    if not org:
        org_variants = variants.get('organizations', [])
        org = org_variants[0] if org_variants else None
    
    if not role:
        role_variants = variants.get('roles', [])
        role = role_variants[0] if role_variants else None
    
    locs = event.get('locations', [])
    if not locs:
        loc_variants = variants.get('locations', [])
        locs = loc_variants[:2] if loc_variants else []
    
    return {
        'organization': org,
        'role': role,
        'locations': locs,
        'using_variants': (org in variants.get('organizations', [])) or (role in variants.get('roles', []))
    }

def render_event_card(event: Dict[str, Any]):
    decision = event.get("consolidation_status", "unknown")
    event_type = event.get("event_type", "N/A")
    
    if decision == "same_event":
        badge = "🔗 MERGED"
        color = "blue"
    elif decision == "singleton":
        badge = "📌 SINGLE"
        color = "gray"
    else:
        badge = "🔀 DIFFERENT"
        color = "orange"
    
    type_emoji = "💼" if event_type == "career_position" else "🏆"
    
    display = get_display_values(event)
    role = display['role'] or 'N/A'
    org = display['organization'] or 'N/A'
    
    col1, col2, col3 = st.columns([2, 3, 1])
    
    with col1:
        st.markdown(f"**{event.get('time_period', 'Unknown')}**")
    
    with col2:
        st.markdown(f"{type_emoji} **{role}**")
        st.caption(f"at {org}")
        if display['using_variants']:
            st.caption("⚠️ Using variant data")
    
    with col3:
        st.markdown(f":{color}[{badge}]")
        st.caption(f"{event.get('sources', {}).get('chunk_count', 0)} sources")
    
    with st.expander("📋 Full Details"):
        tab1, tab2, tab3 = st.tabs(["Core Info", "Variants", "Provenance"])
        
        with tab1:
            st.markdown("### Event Details")
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.markdown(f"**Type:** {event_type}")
                st.markdown(f"**Organization:** {org}")
                st.markdown(f"**Role:** {role}")
            
            with col_b:
                locs_display = ', '.join(display['locations']) if display['locations'] else 'N/A'
                st.markdown(f"**Locations:** {locs_display}")
                st.markdown(f"**Start Year:** {event.get('start_year', 'Unknown')}")
                st.markdown(f"**End Year:** {event.get('end_year', 'Unknown')}")
            
            additional = event.get('additional_details', [])
            if additional:
                st.markdown("**Additional Details:**")
                for detail in additional:
                    st.markdown(f"- {detail}")
            
            if display['using_variants']:
                st.warning("⚠️ Main display is using variant data because canonical representation was incomplete.")
        
        with tab2:
            st.markdown("### All Variants")
            
            variants = event.get('variants', {})
            
            orgs = variants.get('organizations', [])
            if orgs:
                st.markdown("**Organization Variants:**")
                for i, org_var in enumerate(orgs, 1):
                    st.markdown(f"{i}. {org_var}")
            else:
                st.info("No organization variants")
            
            roles = variants.get('roles', [])
            if roles:
                st.markdown("**Role Variants:**")
                for i, role_var in enumerate(roles, 1):
                    st.markdown(f"{i}. {role_var}")
            else:
                st.info("No role variants")
            
            locs = variants.get('locations', [])
            if locs:
                st.markdown("**Location Variants:**")
                for i, loc_var in enumerate(locs, 1):
                    st.markdown(f"{i}. {loc_var}")
        
        with tab3:
            st.markdown("### Source Provenance")
            
            sources = event.get('sources', {})
            
            st.metric("Source Events", sources.get('chunk_count', 0))
            
            chunk_ids = sources.get('chunk_ids', [])
            if chunk_ids:
                st.markdown("**Source Chunks:**")
                st.code(", ".join(map(str, chunk_ids)))
            
            urls = sources.get('urls', [])
            if urls:
                st.markdown("**Source URLs:**")
                for url in urls[:5]:
                    st.markdown(f"- [{url}]({url})")
                if len(urls) > 5:
                    st.caption(f"... and {len(urls) - 5} more")
            
            confidence = event.get('confidence', {})
            if confidence:
                st.markdown("**Consolidation Confidence:**")
                st.markdown(f"- Level: {confidence.get('consolidation_confidence', 'N/A')}")
                reasoning = confidence.get('reasoning', '')
                if reasoning:
                    with st.expander("View reasoning"):
                        st.markdown(reasoning)

def main():
    st.title("Career Timeline Viewer")
    st.markdown("---")
    
    script_dir = Path(__file__).parent
    timeline_file = script_dir / "outputs" / "04_final_timeline.json"
    
    if not timeline_file.exists():
        st.error(f"Timeline file not found: {timeline_file}")
        st.info("Run pipeline.py first to generate the timeline.")
        return
    
    data = load_timeline(timeline_file)
    
    person_name = data.get("person_name", "Unknown")
    summary = data.get("timeline_summary", {})
    timeline = data.get("timeline", [])
    
    st.header(f"📊 {person_name}")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Events", summary.get("total_events", 0))
    
    with col2:
        st.metric("With Dates", summary.get("with_time", 0))
    
    with col3:
        st.metric("Career Positions", summary.get("career_positions", 0))
    
    with col4:
        st.metric("Awards", summary.get("awards", 0))
    
    with col5:
        incomplete = sum(1 for e in timeline if not e.get("organization") or not e.get("role"))
        st.metric("Incomplete", incomplete)
    
    if incomplete > 0:
        st.warning(f"⚠️ {incomplete} events have incomplete data (missing organization or role). Use the 'Data Completeness' filter to review them.")
    
    year_range = summary.get("year_range")
    if year_range:
        st.info(f"📅 Timeline spans: {year_range[0]} - {year_range[1]}")
    
    st.markdown("---")
    
    st.sidebar.header("Filters")
    
    event_type_filter = st.sidebar.multiselect(
        "Event Type",
        ["career_position", "award"],
        default=["career_position", "award"]
    )
    
    consolidation_filter = st.sidebar.multiselect(
        "Consolidation Status",
        ["same_event", "singleton", "different_events"],
        default=["same_event", "singleton", "different_events"]
    )
    
    time_filter = st.sidebar.radio(
        "Time Information",
        ["All", "With dates only", "Without dates only"]
    )
    
    completeness_filter = st.sidebar.radio(
        "Data Completeness",
        ["All", "Complete (org + role)", "Incomplete (missing org or role)"]
    )
    
    search_term = st.sidebar.text_input("Search in org/role")
    
    filtered_timeline = timeline
    
    if event_type_filter:
        filtered_timeline = [
            e for e in filtered_timeline 
            if e.get("event_type") in event_type_filter
        ]
    
    if consolidation_filter:
        filtered_timeline = [
            e for e in filtered_timeline
            if e.get("consolidation_status") in consolidation_filter
        ]
    
    if time_filter == "With dates only":
        filtered_timeline = [
            e for e in filtered_timeline
            if e.get("start_year") is not None
        ]
    elif time_filter == "Without dates only":
        filtered_timeline = [
            e for e in filtered_timeline
            if e.get("start_year") is None
        ]
    
    if completeness_filter == "Complete (org + role)":
        filtered_timeline = [
            e for e in filtered_timeline
            if e.get("organization") and e.get("role")
        ]
    elif completeness_filter == "Incomplete (missing org or role)":
        filtered_timeline = [
            e for e in filtered_timeline
            if not e.get("organization") or not e.get("role")
        ]
    
    if search_term:
        search_lower = search_term.lower()
        filtered_timeline = [
            e for e in filtered_timeline
            if (search_lower in (e.get("organization") or "").lower() or
                search_lower in (e.get("role") or "").lower())
        ]
    
    st.sidebar.markdown(f"**Showing {len(filtered_timeline)} / {len(timeline)} events**")
    
    if st.sidebar.button("🔄 Reload Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.header(f"Timeline ({len(filtered_timeline)} events)")
    
    if not filtered_timeline:
        st.warning("No events match the current filters.")
        return
    
    for event in filtered_timeline:
        render_event_card(event)
        st.markdown("---")

if __name__ == "__main__":
    main()