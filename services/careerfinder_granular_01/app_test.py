import json
import streamlit as st
import re
from pathlib import Path
from typing import Dict, Any, List

from classification import classify_chunk
from extraction_step1 import extract_entities_step1
from extraction_step2 import assemble_events_step2
from extraction_step3 import verify_events_step3
from load_data import load_chunks_from_db, get_all_people

st.set_page_config(page_title="CareerFinder Granular - Test Interface", layout="wide")

def highlight_quotes_in_text(text: str, quotes: List[str], color: str = "yellow") -> str:
    highlighted = text
    for quote in quotes:
        if quote and len(quote) > 5:
            escaped_quote = re.escape(quote)
            pattern = re.compile(f"({escaped_quote})", re.IGNORECASE)
            highlighted = pattern.sub(f'<mark style="background-color: {color};">{quote}</mark>', highlighted, count=1)
    return highlighted

def resolve_event_display(event: Dict[str, Any], entities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    resolved = {}
    
    time_ids = event.get("time_marker_ids", [])
    org_ids = event.get("organization_ids", [])
    role_ids = event.get("role_ids", [])
    loc_ids = event.get("location_ids", [])
    
    resolved["time_markers"] = [entities["time_markers"][i].get("text") for i in time_ids if i < len(entities["time_markers"])]
    resolved["organizations"] = [entities["organizations"][i].get("name") for i in org_ids if i < len(entities["organizations"])]
    resolved["roles"] = [entities["roles"][i].get("title") for i in role_ids if i < len(entities["roles"])]
    resolved["locations"] = [entities["locations"][i].get("place") for i in loc_ids if i < len(entities["locations"])]
    
    return resolved

def initialize_session_state():
    if "chunk_loaded" not in st.session_state:
        st.session_state.chunk_loaded = False
    if "current_chunk" not in st.session_state:
        st.session_state.current_chunk = None
    if "step1_results" not in st.session_state:
        st.session_state.step1_results = None
    if "step2_results" not in st.session_state:
        st.session_state.step2_results = None
    if "step3_results" not in st.session_state:
        st.session_state.step3_results = None
    if "selected_entity_quotes" not in st.session_state:
        st.session_state.selected_entity_quotes = []

initialize_session_state()

st.title("CareerFinder Granular - Visual Testing Interface")

tab1, tab2, tab3 = st.tabs(["Setup", "Step 1: Entity Extraction", "Steps 2-3: Assembly & Verification"])

with tab1:
    st.header("Load Chunk for Testing")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Option 1: Load from Database")
        
        try:
            people = get_all_people()
            selected_person = st.selectbox("Select person", people)
            
            if st.button("Load chunks for this person"):
                chunks = load_chunks_from_db(selected_person)
                st.session_state.available_chunks = chunks
                st.success(f"Loaded {len(chunks)} chunks")
            
            if hasattr(st.session_state, "available_chunks"):
                chunk_options = [
                    f"Chunk {i+1}: {c.get('title', 'No title')[:50]}... (ID: {c.get('chunk_id')})"
                    for i, c in enumerate(st.session_state.available_chunks)
                ]
                selected_idx = st.selectbox("Select chunk", range(len(chunk_options)), format_func=lambda x: chunk_options[x])
                
                if st.button("Load Selected Chunk"):
                    st.session_state.current_chunk = st.session_state.available_chunks[selected_idx]
                    st.session_state.chunk_loaded = True
                    st.session_state.step1_results = None
                    st.session_state.step2_results = None
                    st.session_state.step3_results = None
                    st.rerun()
        
        except Exception as e:
            st.error(f"Database error: {e}")
            st.info("Use Option 2 to paste JSON directly")
    
    with col2:
        st.subheader("Option 2: Paste Chunk JSON")
        
        chunk_json = st.text_area("Paste chunk JSON here", height=300)
        
        if st.button("Load from JSON"):
            try:
                chunk = json.loads(chunk_json)
                st.session_state.current_chunk = chunk
                st.session_state.chunk_loaded = True
                st.session_state.step1_results = None
                st.session_state.step2_results = None
                st.session_state.step3_results = None
                st.success("Chunk loaded successfully")
                st.rerun()
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")
    
    if st.session_state.chunk_loaded:
        st.divider()
        st.subheader("Current Chunk Info")
        
        chunk = st.session_state.current_chunk
        doc_type = classify_chunk(chunk)
        
        col_info1, col_info2, col_info3 = st.columns(3)
        
        with col_info1:
            st.metric("Chunk ID", chunk.get("chunk_id", "N/A"))
        with col_info2:
            st.metric("Document Type", doc_type)
        with col_info3:
            st.metric("Text Length", len(chunk.get("text", "")))
        
        st.caption(f"Source: {chunk.get('source_url', 'Unknown')}")
        st.caption(f"Title: {chunk.get('title', 'Unknown')}")

with tab2:
    st.header("Step 1: Entity Extraction")
    
    if not st.session_state.chunk_loaded:
        st.warning("Please load a chunk in the Setup tab first")
    else:
        chunk = st.session_state.current_chunk
        chunk_text = chunk.get("text", "")
        doc_type = classify_chunk(chunk)
        
        col_left, col_right = st.columns([1, 1])
        
        with col_left:
            st.subheader("Controls")
            
            controls_container = st.container(height=800)
            
            with controls_container:
                prompt_variant = st.selectbox(
                    "Prompt variant",
                    ["Auto-detect", "cv_structured", "narrative"],
                    help="Auto-detect uses document type classification"
                )
                
                if prompt_variant == "Auto-detect":
                    prompt_to_use = None
                    st.info(f"Will use: step1_{doc_type if doc_type == 'cv_structured' else 'narrative'}")
                else:
                    prompt_to_use = prompt_variant
                
                if st.button("Run Step 1 Extraction", type="primary"):
                    with st.spinner("Extracting entities..."):
                        try:
                            config_path = Path(__file__).parent / "config" / "config.json"
                            result = extract_entities_step1(
                                chunk_text,
                                chunk,
                                config_path,
                                prompt_to_use
                            )
                            st.session_state.step1_results = result
                            st.session_state.step2_results = None
                            st.session_state.step3_results = None
                            st.session_state.selected_entity_quotes = []
                            st.success("Extraction complete!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Extraction failed: {e}")
                
                if st.session_state.step1_results:
                    st.divider()
                    
                    results = st.session_state.step1_results
                    entities = results["entities"]
                    
                    st.metric("Prompt Used", results["prompt_used"])
                    
                    col_m1, col_m2 = st.columns(2)
                    with col_m1:
                        st.metric("Time Markers", len(entities["time_markers"]))
                        st.metric("Organizations", len(entities["organizations"]))
                    with col_m2:
                        st.metric("Roles", len(entities["roles"]))
                        st.metric("Locations", len(entities["locations"]))
                    
                    st.divider()
                    st.subheader("Extracted Entities")
                    
                    entity_type = st.selectbox(
                        "View entity type",
                        ["time_markers", "organizations", "roles", "locations"]
                    )
                    
                    entities_list = entities[entity_type]
                    
                    if entities_list:
                        for i, entity in enumerate(entities_list):
                            with st.expander(f"{i+1}. {entity.get('text') or entity.get('name') or entity.get('title') or entity.get('place')}"):
                                st.json(entity)
                                
                                if st.button(f"Highlight in text", key=f"highlight_{entity_type}_{i}"):
                                    st.session_state.selected_entity_quotes = entity.get("quotes", [])
                                    st.rerun()
                    else:
                        st.info(f"No {entity_type} extracted")
                    
                    with st.expander("View Raw LLM Output"):
                        st.code(results["raw_llm_output"], language="json")
        
        with col_right:
            st.subheader("Source Text")
            
            display_text = chunk_text
            
            if st.session_state.step1_results:
                entities = st.session_state.step1_results["entities"]
                
                if st.session_state.selected_entity_quotes:
                    display_text = highlight_quotes_in_text(
                        chunk_text,
                        st.session_state.selected_entity_quotes,
                        "yellow"
                    )
                else:
                    all_quotes = []
                    
                    for tm in entities.get("time_markers", []):
                        for quote in tm.get("quotes", []):
                            all_quotes.append((quote, "yellow"))
                    
                    for org in entities.get("organizations", []):
                        for quote in org.get("quotes", []):
                            all_quotes.append((quote, "lightblue"))
                    
                    for role in entities.get("roles", []):
                        for quote in role.get("quotes", []):
                            all_quotes.append((quote, "lightgreen"))
                    
                    for loc in entities.get("locations", []):
                        for quote in loc.get("quotes", []):
                            all_quotes.append((quote, "pink"))
                    
                    for quote, color in all_quotes:
                        if quote and len(quote) > 5:
                            escaped_quote = re.escape(quote)
                            pattern = re.compile(f"({escaped_quote})", re.IGNORECASE)
                            display_text = pattern.sub(f'<mark style="background-color: {color};">{quote}</mark>', display_text, count=1)
            
            st.markdown(
                f'<div style="background-color: white; color: black; padding: 20px; border: 1px solid #ddd; height: 800px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap; line-height: 1.6;">{display_text}</div>',
                unsafe_allow_html=True
            )
            
            if st.session_state.step1_results:
                st.caption("🟡 Time Markers | 🔵 Organizations | 🟢 Roles | 🩷 Locations")
            
            if st.session_state.selected_entity_quotes:
                if st.button("Clear highlighting"):
                    st.session_state.selected_entity_quotes = []
                    st.rerun()

with tab3:
    st.header("Steps 2 & 3: Event Assembly and Verification")
    
    if not st.session_state.chunk_loaded:
        st.warning("Please load a chunk in the Setup tab first")
    else:
        chunk = st.session_state.current_chunk
        chunk_text = chunk.get("text", "")
        doc_type = classify_chunk(chunk)
        
        st.markdown("### Execution Mode")
        
        col_mode1, col_mode2 = st.columns(2)
        
        with col_mode1:
            if st.button("▶️ Run Complete Pipeline (Steps 1→2→3)", type="primary"):
                config_path = Path(__file__).parent / "config" / "config.json"
                
                with st.status("Running complete pipeline...", expanded=True) as status:
                    try:
                        st.write("🔄 Step 1: Extracting entities...")
                        step1_result = extract_entities_step1(
                            chunk_text,
                            chunk,
                            config_path,
                            None
                        )
                        st.session_state.step1_results = step1_result
                        
                        entities = step1_result["entities"]
                        total_entities = sum(len(entities[k]) for k in ["time_markers", "organizations", "roles", "locations"])
                        st.write(f"✅ Step 1 complete: {total_entities} entities extracted")
                        
                        st.write("🔄 Step 2: Assembling events...")
                        step2_result = assemble_events_step2(entities, config_path)
                        st.session_state.step2_results = step2_result
                        
                        events = step2_result["assembled_events"]
                        career_positions = sum(1 for e in events if e.get("event_type") == "career_position")
                        awards = sum(1 for e in events if e.get("event_type") == "award")
                        
                        st.write(f"✅ Step 2 complete: {len(events)} events ({career_positions} career, {awards} awards)")
                        
                        st.write("🔄 Step 3: Verifying events...")
                        step3_result = verify_events_step3(events, entities, config_path)
                        st.session_state.step3_results = step3_result
                        
                        summary = step3_result["summary"]
                        st.write(f"✅ Step 3 complete: {summary.get('valid', 0)} valid, {summary.get('warnings', 0)} warnings, {summary.get('errors', 0)} errors")
                        
                        status.update(label="Complete pipeline finished!", state="complete", expanded=False)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Pipeline failed: {e}")
                        status.update(label="Pipeline failed", state="error")
        
        with col_mode2:
            if st.session_state.step1_results and st.button("▶️ Run Steps 2→3 Only"):
                config_path = Path(__file__).parent / "config" / "config.json"
                
                with st.status("Running steps 2-3...", expanded=True) as status:
                    try:
                        st.write("🔄 Step 2: Assembling events...")
                        entities = st.session_state.step1_results["entities"]
                        step2_result = assemble_events_step2(entities, config_path)
                        st.session_state.step2_results = step2_result
                        
                        events = step2_result["assembled_events"]
                        career_positions = sum(1 for e in events if e.get("event_type") == "career_position")
                        awards = sum(1 for e in events if e.get("event_type") == "award")
                        
                        st.write(f"✅ Step 2 complete: {len(events)} events ({career_positions} career, {awards} awards)")
                        
                        st.write("🔄 Step 3: Verifying events...")
                        step3_result = verify_events_step3(events, entities, config_path)
                        st.session_state.step3_results = step3_result
                        
                        summary = step3_result["summary"]
                        st.write(f"✅ Step 3 complete: {summary.get('valid', 0)} valid, {summary.get('warnings', 0)} warnings, {summary.get('errors', 0)} errors")
                        
                        status.update(label="Steps 2-3 complete!", state="complete", expanded=False)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Pipeline failed: {e}")
                        status.update(label="Pipeline failed", state="error")
        
        if not st.session_state.step1_results:
            st.info("💡 Click 'Run Complete Pipeline' above to extract and assemble all events, or go to Step 1 tab to run extraction only.")
        
        st.divider()
        
        col_left, col_right = st.columns([1, 1])
        
        with col_left:
            st.subheader("Controls")
            
            controls_container = st.container(height=800)
            
            with controls_container:
                st.markdown("### Step 2: Event Assembly")
                
                if st.button("Run Step 2 Only", disabled=not st.session_state.step1_results):
                    with st.spinner("Assembling events..."):
                        try:
                            config_path = Path(__file__).parent / "config" / "config.json"
                            entities = st.session_state.step1_results["entities"]
                            result = assemble_events_step2(entities, config_path)
                            st.session_state.step2_results = result
                            st.session_state.step3_results = None
                            st.success("Assembly complete!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Assembly failed: {e}")
                
                if st.session_state.step2_results:
                    st.divider()
                    
                    events = st.session_state.step2_results["assembled_events"]
                    
                    st.metric("Assembled Events", len(events))
                    
                    career_positions = sum(1 for e in events if e.get("event_type") == "career_position")
                    awards = sum(1 for e in events if e.get("event_type") == "award")
                    
                    col_m1, col_m2 = st.columns(2)
                    with col_m1:
                        st.metric("Career Positions", career_positions)
                    with col_m2:
                        st.metric("Awards", awards)
                    
                    st.divider()
                    st.markdown("### Step 3: Verification")
                    
                    if st.button("Run Step 3 Only", disabled=not st.session_state.step2_results):
                        with st.spinner("Verifying events..."):
                            try:
                                config_path = Path(__file__).parent / "config" / "config.json"
                                entities = st.session_state.step1_results["entities"]
                                events = st.session_state.step2_results["assembled_events"]
                                result = verify_events_step3(events, entities, config_path)
                                st.session_state.step3_results = result
                                st.success("Verification complete!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Verification failed: {e}")
                    
                    if st.session_state.step3_results:
                        st.divider()
                        
                        summary = st.session_state.step3_results["summary"]
                        
                        col_s1, col_s2 = st.columns(2)
                        with col_s1:
                            st.metric("Valid", summary.get("valid", 0))
                            st.metric("Warnings", summary.get("warnings", 0))
                        with col_s2:
                            st.metric("Errors", summary.get("errors", 0))
                            st.metric("Total", summary.get("total_events", 0))
                    
                    st.divider()
                    st.subheader("Assembled Events")
                    
                    filter_type = st.selectbox(
                        "Filter by type",
                        ["All", "career_position", "award"]
                    )
                    
                    filtered_events = events
                    if filter_type != "All":
                        filtered_events = [e for e in events if e.get("event_type") == filter_type]
                    
                    for i, event in enumerate(filtered_events):
                        event_type = event.get("event_type", "unknown")
                        confidence = event.get("confidence", "unknown")
                        
                        type_emoji = "💼" if event_type == "career_position" else "🏆"
                        
                        entities_data = st.session_state.step1_results["entities"]
                        resolved = resolve_event_display(event, entities_data)
                        
                        org_display = ", ".join(resolved["organizations"]) if resolved["organizations"] else "N/A"
                        role_display = ", ".join(resolved["roles"]) if resolved["roles"] else "N/A"
                        loc_display = ", ".join(resolved["locations"]) if resolved["locations"] else "N/A"
                        time_display = ", ".join(resolved["time_markers"]) if resolved["time_markers"] else "N/A"
                        
                        event_summary = f"{org_display} | {role_display}"
                        
                        with st.expander(f"{type_emoji} Event {i+1}: {event_summary}"):
                            st.markdown(f"**Organization:** {org_display}")
                            st.markdown(f"**Role:** {role_display}")
                            st.markdown(f"**Location:** {loc_display}")
                            st.markdown(f"**Time:** {time_display}")
                            st.markdown(f"**Event Type:** {event_type}")
                            st.markdown(f"**Confidence:** {confidence}")
                            
                            if event.get("notes"):
                                st.markdown(f"**Notes:** {event.get('notes')}")
                            
                            st.divider()
                            
                            if st.session_state.step3_results:
                                verified = st.session_state.step3_results["verified_events"]
                                if i < len(verified):
                                    v = verified[i]
                                    if v.get("issues"):
                                        st.warning(f"Status: {v.get('status')}")
                                        for issue in v["issues"]:
                                            st.markdown(f"- **[{issue['severity']}]** {issue['type']}: {issue['description']}")
                                    else:
                                        st.success(f"Status: {v.get('status')}")
                            
                            with st.expander("View Full JSON"):
                                st.json(event)
                            
                            if st.button(f"Highlight quotes", key=f"event_quotes_{i}"):
                                st.session_state.selected_entity_quotes = event.get("supporting_quotes", [])
                                st.rerun()
                    
                    with st.expander("View Step 2 Raw Output"):
                        st.code(st.session_state.step2_results["raw_llm_output"], language="json")
                    
                    if st.session_state.step3_results:
                        with st.expander("View Step 3 Raw Output"):
                            st.code(st.session_state.step3_results["raw_llm_output"], language="json")
        
        with col_right:
            st.subheader("Source Text")
            
            display_text = chunk_text
            
            if st.session_state.selected_entity_quotes:
                display_text = highlight_quotes_in_text(
                    chunk_text,
                    st.session_state.selected_entity_quotes,
                    "yellow"
                )
            
            st.markdown(
                f'<div style="background-color: white; color: black; padding: 20px; border: 1px solid #ddd; height: 800px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap; line-height: 1.6;">{display_text}</div>',
                unsafe_allow_html=True
            )
            
            if st.session_state.selected_entity_quotes:
                if st.button("Clear highlighting"):
                    st.session_state.selected_entity_quotes = []
                    st.rerun()