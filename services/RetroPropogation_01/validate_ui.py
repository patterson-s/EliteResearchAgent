import streamlit as st
import json
import zipfile
import shutil
from pathlib import Path
from typing import Dict, Any, List
import tempfile

st.set_page_config(page_title="Career Event Validation", layout="wide")

def extract_uploaded_data(uploaded_file) -> Path:
    temp_dir = Path(tempfile.mkdtemp())
    
    with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    person_dirs = [d for d in temp_dir.iterdir() if d.is_dir()]
    if person_dirs:
        return person_dirs[0]
    return temp_dir

def get_available_people(outputs_dir: Path) -> List[str]:
    if not outputs_dir.exists():
        return []
    return [d.name for d in outputs_dir.iterdir() if d.is_dir()]

def load_pipeline_data(person_dir: Path) -> Dict[str, Any]:
    with open(person_dir / "step1_entities.json", "r", encoding="utf-8") as f:
        step1 = json.load(f)
    
    with open(person_dir / "step2_canonical_orgs.json", "r", encoding="utf-8") as f:
        step2 = json.load(f)
    
    with open(person_dir / "step3_events.json", "r", encoding="utf-8") as f:
        step3 = json.load(f)
    
    with open(person_dir / "step4_verification.json", "r", encoding="utf-8") as f:
        step4 = json.load(f)
    
    return {
        "entities": step1["entities"],
        "chunk_results": step1.get("chunk_results", []),
        "canonical_orgs": step2["canonical_organizations"],
        "events": step3["events"],
        "verification": step4["verified_events"]
    }

def sort_events_chronologically(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def get_start_year(event):
        time_period = event.get("time_period", {})
        start = time_period.get("start")
        if start:
            try:
                return int(start.split("-")[0])
            except:
                pass
        return 9999
    
    return sorted(events, key=get_start_year)

def get_verification_status(event_id: str, verifications: List[Dict]) -> Dict:
    for v in verifications:
        if v["event_id"] == event_id:
            return v
    return {"status": "unknown", "issues": []}

def render_event_card(event: Dict[str, Any], canonical_orgs: List[Dict], event_idx: int):
    st.markdown(f"<h1 style='font-size: 2.5rem; margin-bottom: 0.5rem;'>Event {event_idx + 1}: {event['event_id']}</h1>", unsafe_allow_html=True)
    
    event_type_label = "CAREER POSITION" if event['event_type'] == "career_position" else "AWARD"
    st.markdown(f"<p style='font-size: 1.2rem; color: #666;'><strong>Type:</strong> {event_type_label}</p>", unsafe_allow_html=True)
    
    st.markdown("<hr style='margin: 1.5rem 0;'>", unsafe_allow_html=True)
    
    col_valid, col_notes = st.columns([1, 3])
    with col_valid:
        event_valid = st.checkbox(
            "Event is valid", 
            value=True, 
            key=f"event_valid_{event['event_id']}"
        )
    
    if not event_valid:
        with col_notes:
            st.text_area(
                "Why invalid?",
                key=f"event_notes_{event['event_id']}",
                placeholder="Explain issue...",
                height=100
            )
    
    st.markdown("<hr style='margin: 1.5rem 0;'>", unsafe_allow_html=True)
    
    org_id = event.get("canonical_org_id")
    if org_id:
        org = next((o for o in canonical_orgs if o["canonical_id"] == org_id), None)
        if org:
            st.markdown(f"<h3 style='font-size: 1.8rem; margin-top: 1rem;'>Organization</h3>", unsafe_allow_html=True)
            st.markdown(f"<p style='font-size: 1.4rem; margin: 0.5rem 0;'>{org['canonical_name']}</p>", unsafe_allow_html=True)
            
            col1, col2 = st.columns([1, 3])
            with col1:
                org_valid = st.checkbox(
                    "Correct",
                    value=True,
                    key=f"org_valid_{event['event_id']}"
                )
            if not org_valid:
                with col2:
                    st.text_input(
                        "Correct organization",
                        key=f"org_correction_{event['event_id']}",
                        placeholder="Enter correct name"
                    )
    
    time_period = event.get("time_period", {})
    if time_period:
        st.markdown(f"<h3 style='font-size: 1.8rem; margin-top: 1rem;'>Time Period</h3>", unsafe_allow_html=True)
        time_str = time_period.get("text", "N/A")
        st.markdown(f"<p style='font-size: 1.4rem; margin: 0.5rem 0;'>{time_str}</p>", unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            time_valid = st.checkbox(
                "Correct",
                value=True,
                key=f"time_valid_{event['event_id']}"
            )
        if not time_valid:
            with col2:
                st.text_input(
                    "Correct time period",
                    key=f"time_correction_{event['event_id']}",
                    placeholder="YYYY-YYYY"
                )
    
    roles = event.get("roles", [])
    if roles:
        st.markdown(f"<h3 style='font-size: 1.8rem; margin-top: 1rem;'>Roles</h3>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size: 1.4rem; margin: 0.5rem 0;'>{', '.join(roles)}</p>", unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            roles_valid = st.checkbox(
                "Correct",
                value=True,
                key=f"roles_valid_{event['event_id']}"
            )
        if not roles_valid:
            with col2:
                st.text_input(
                    "Correct roles",
                    key=f"roles_correction_{event['event_id']}",
                    placeholder="Enter correct roles"
                )
    
    locations = event.get("locations", [])
    if locations:
        st.markdown(f"<h3 style='font-size: 1.8rem; margin-top: 1rem;'>Locations</h3>", unsafe_allow_html=True)
        st.markdown(f"<p style='font-size: 1.4rem; margin: 0.5rem 0;'>{', '.join(locations)}</p>", unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            locations_valid = st.checkbox(
                "Correct",
                value=True,
                key=f"locations_valid_{event['event_id']}"
            )
        if not locations_valid:
            with col2:
                st.text_input(
                    "Correct locations",
                    key=f"locations_correction_{event['event_id']}",
                    placeholder="Enter correct locations"
                )
    
    st.markdown(f"<p style='font-size: 1.1rem; margin-top: 1rem;'><strong>Confidence:</strong> {event.get('confidence', 'N/A')}</p>", unsafe_allow_html=True)
    
    notes = event.get("notes")
    if notes:
        st.info(f"**Assembly Notes:** {notes}")

def render_source_panel(event: Dict[str, Any], chunk_results: List[Dict]):
    st.markdown(f"<h1 style='font-size: 2.5rem; margin-bottom: 1rem;'>Supporting Sources</h1>", unsafe_allow_html=True)
    
    quotes = event.get("supporting_quotes", [])
    
    if not quotes:
        st.warning("No supporting quotes found")
        return
    
    st.markdown(f"<p style='font-size: 1.2rem; margin-bottom: 1rem;'><strong>{len(quotes)} supporting quotes:</strong></p>", unsafe_allow_html=True)
    
    for i, quote in enumerate(quotes, 1):
        with st.expander(f"Quote {i}", expanded=(i <= 3)):
            st.markdown(f"<p style='font-size: 1.2rem; font-style: italic; line-height: 1.6;'>{quote}</p>", unsafe_allow_html=True)

def main():
    st.title("Career Event Validation Interface")
    
    script_dir = Path(__file__).parent
    outputs_dir = script_dir / "outputs"
    
    st.sidebar.title("Data Source")
    
    data_source = st.sidebar.radio(
        "Choose data source:",
        ["Local outputs folder", "Upload ZIP file"],
        key="data_source"
    )
    
    person_dir = None
    
    if data_source == "Upload ZIP file":
        st.sidebar.markdown("---")
        uploaded_file = st.sidebar.file_uploader(
            "Upload person data (ZIP)",
            type=['zip'],
            help="Upload a ZIP file containing step1-4 JSON files"
        )
        
        if uploaded_file:
            try:
                person_dir = extract_uploaded_data(uploaded_file)
                st.sidebar.success(f"Loaded: {person_dir.name}")
            except Exception as e:
                st.sidebar.error(f"Error loading ZIP: {e}")
                return
        else:
            st.info("Please upload a ZIP file containing the person's data files (step1_entities.json, step2_canonical_orgs.json, step3_events.json, step4_verification.json)")
            return
    
    else:
        if not outputs_dir.exists():
            st.error(f"Outputs directory not found: {outputs_dir}")
            return
        
        available_people = get_available_people(outputs_dir)
        
        if not available_people:
            st.sidebar.error("No data found in outputs directory")
            return
        
        selected_person = st.sidebar.selectbox(
            "Select person:",
            available_people,
            key="selected_person"
        )
        
        person_dir = outputs_dir / selected_person
    
    try:
        data = load_pipeline_data(person_dir)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return
    
    events = sort_events_chronologically(data["events"])
    
    st.sidebar.markdown("---")
    st.sidebar.title("Navigation")
    
    if "event_idx" not in st.session_state:
        st.session_state.event_idx = 0
    
    event_idx = st.session_state.event_idx
    
    if event_idx >= len(events):
        event_idx = 0
        st.session_state.event_idx = 0
    
    st.sidebar.markdown(f"**Event {event_idx + 1} of {len(events)}**")
    
    event_options = [f"Event {i+1}: {e['event_id']}" for i, e in enumerate(events)]
    selected_idx = st.sidebar.selectbox(
        "Jump to event:",
        range(len(events)),
        index=event_idx,
        format_func=lambda i: event_options[i],
        key="event_selector"
    )
    
    if selected_idx != event_idx:
        st.session_state.event_idx = selected_idx
        st.rerun()
    
    verification = get_verification_status(
        events[event_idx]["event_id"],
        data["verification"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Status:** {verification['status']}")
    
    if verification.get("issues"):
        st.sidebar.markdown("**Issues:**")
        for issue in verification["issues"]:
            st.sidebar.markdown(f"- [{issue['severity']}] {issue['type']}")
    
    st.sidebar.markdown("---")
    st.sidebar.title("Download")
    
    validation_file = person_dir / "validations.json"
    if validation_file.exists():
        with open(validation_file, "r", encoding="utf-8") as f:
            validations_data = f.read()
        
        st.sidebar.download_button(
            label="Download Validations (JSON)",
            data=validations_data,
            file_name=f"{person_dir.name}_validations.json",
            mime="application/json",
            use_container_width=True
        )
    else:
        st.sidebar.info("No validations saved yet")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        render_event_card(
            events[event_idx],
            data["canonical_orgs"],
            event_idx
        )
    
    with col2:
        render_source_panel(
            events[event_idx],
            data["chunk_results"]
        )
    
    st.markdown("---")
    
    col_prev, col_save, col_next = st.columns([1, 2, 1])
    
    with col_prev:
        if st.button("Previous", disabled=(event_idx == 0), use_container_width=True):
            st.session_state.event_idx = event_idx - 1
            st.rerun()
    
    with col_save:
        if st.button("Save Validation", type="primary", use_container_width=True):
            validation_data = {
                "event_id": events[event_idx]["event_id"],
                "event_valid": st.session_state.get(f"event_valid_{events[event_idx]['event_id']}", True),
                "org_valid": st.session_state.get(f"org_valid_{events[event_idx]['event_id']}", True),
                "time_valid": st.session_state.get(f"time_valid_{events[event_idx]['event_id']}", True),
                "roles_valid": st.session_state.get(f"roles_valid_{events[event_idx]['event_id']}", True),
                "locations_valid": st.session_state.get(f"locations_valid_{events[event_idx]['event_id']}", True),
                "notes": st.session_state.get(f"event_notes_{events[event_idx]['event_id']}", "")
            }
            
            validation_file = person_dir / "validations.json"
            
            if validation_file.exists():
                with open(validation_file, "r", encoding="utf-8") as f:
                    all_validations = json.load(f)
            else:
                all_validations = []
            
            existing = next((v for v in all_validations if v["event_id"] == validation_data["event_id"]), None)
            if existing:
                all_validations.remove(existing)
            all_validations.append(validation_data)
            
            with open(validation_file, "w", encoding="utf-8") as f:
                json.dump(all_validations, f, indent=2, ensure_ascii=False)
            
            st.success("Validation saved!")
    
    with col_next:
        if st.button("Next", disabled=(event_idx == len(events) - 1), use_container_width=True):
            st.session_state.event_idx = event_idx + 1
            st.rerun()
    
    progress = (event_idx + 1) / len(events)
    st.progress(progress, text=f"Progress: {event_idx + 1}/{len(events)} events")

if __name__ == "__main__":
    main()