import json
import streamlit as st
from typing import List, Dict, Any
import re

st.set_page_config(page_title="Career Event Evaluator - Team", layout="wide")

def get_events_for_chunk(chunk_id: str, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [e for e in events if str(e.get("chunk_id")) == str(chunk_id)]

def highlight_quotes_in_text(text: str, quotes: List[str]) -> str:
    highlighted = text
    for quote in quotes:
        if quote and len(quote) > 5:
            escaped_quote = re.escape(quote)
            pattern = re.compile(f"({escaped_quote})", re.IGNORECASE)
            highlighted = pattern.sub(r'<mark style="background-color: yellow;">\1</mark>', highlighted)
    return highlighted

def get_all_chunks_flat(all_people: List[Dict[str, Any]], all_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_person_chunks = []
    
    for person_data in all_people:
        person_name = person_data.get("person_name")
        raw_extractions = person_data.get("raw_extractions", [])
        
        sample_chunk_ids = set(str(e.get("chunk_id")) for e in raw_extractions if e.get("chunk_id"))
        
        person_chunks = [
            c for c in all_chunks 
            if c.get("person_name") == person_name and str(c.get("chunk_id")) in sample_chunk_ids
        ]
        
        all_person_chunks.extend(person_chunks)
    
    return all_person_chunks

def get_all_events_flat(all_people: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_events = []
    for person_data in all_people:
        all_events.extend(person_data.get("raw_extractions", []))
    return all_events

def initialize_session_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "all_people" not in st.session_state:
        st.session_state.all_people = None
    if "chunks_data" not in st.session_state:
        st.session_state.chunks_data = None
    if "current_chunk_idx" not in st.session_state:
        st.session_state.current_chunk_idx = 0
    if "all_chunks_raw" not in st.session_state:
        st.session_state.all_chunks_raw = None
    if "all_events" not in st.session_state:
        st.session_state.all_events = None

initialize_session_state()

st.title("Career Event Evaluator - Team Version")

if not st.session_state.data_loaded:
    st.markdown("### Upload Files to Begin")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Person Result File")
        person_file = st.file_uploader(
            "Upload person result JSON (single or multi-person)",
            type=["json"],
            key="person_upload"
        )
    
    with col2:
        st.subheader("Chunks Data File")
        chunks_file = st.file_uploader(
            "Upload chunks JSON (all_chunks.json)",
            type=["json"],
            key="chunks_upload"
        )
    
    if person_file and chunks_file:
        if st.button("Load Data", type="primary"):
            try:
                person_data_raw = json.load(person_file)
                all_chunks = json.load(chunks_file)
                
                if isinstance(person_data_raw, list):
                    all_people = person_data_raw
                elif isinstance(person_data_raw, dict):
                    all_people = [person_data_raw]
                else:
                    st.error("Invalid file format")
                    st.stop()
                
                for person_data in all_people:
                    if "evaluation_metadata" not in person_data:
                        person_data["evaluation_metadata"] = {
                            "evaluator": "",
                            "evaluator_note": ""
                        }
                
                flat_chunks = get_all_chunks_flat(all_people, all_chunks)
                
                if not flat_chunks:
                    st.error("No chunks found for any person in chunks file")
                    st.stop()
                
                flat_events = get_all_events_flat(all_people)
                
                st.session_state.all_people = all_people
                st.session_state.chunks_data = flat_chunks
                st.session_state.all_chunks_raw = all_chunks
                st.session_state.all_events = flat_events
                st.session_state.data_loaded = True
                st.session_state.current_chunk_idx = 0
                
                st.success(f"Loaded {len(flat_chunks)} chunks across {len(all_people)} people")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error loading files: {e}")
    
    st.stop()

chunks = st.session_state.chunks_data
all_events = st.session_state.all_events
chunk_idx = st.session_state.current_chunk_idx

with st.sidebar:
    st.header("Evaluator Info")
    
    evaluator = st.text_input("Evaluator name", value=st.session_state.all_people[0]["evaluation_metadata"].get("evaluator", ""))
    evaluator_note = st.text_area("Evaluation note", value=st.session_state.all_people[0]["evaluation_metadata"].get("evaluator_note", ""), height=100)
    
    for person_data in st.session_state.all_people:
        person_data["evaluation_metadata"]["evaluator"] = evaluator
        person_data["evaluation_metadata"]["evaluator_note"] = evaluator_note
    
    st.divider()
    st.subheader("Chunk Navigation")
    
    chunk_num_input = st.number_input(
        "Chunk number",
        min_value=1,
        max_value=len(chunks),
        value=chunk_idx + 1,
        step=1,
        key="chunk_nav"
    ) - 1
    
    if chunk_num_input != chunk_idx:
        st.session_state.current_chunk_idx = chunk_num_input
        st.rerun()
    
    col_prev, col_next = st.columns(2)
    with col_prev:
        if st.button("Previous", disabled=(chunk_idx == 0)):
            st.session_state.current_chunk_idx = max(0, chunk_idx - 1)
            st.rerun()
    with col_next:
        if st.button("Next", disabled=(chunk_idx == len(chunks) - 1)):
            st.session_state.current_chunk_idx = min(len(chunks) - 1, chunk_idx + 1)
            st.rerun()
    
    st.divider()
    
    if len(st.session_state.all_people) > 1:
        download_data = st.session_state.all_people
        download_filename = "evaluated_multi_person.json"
    else:
        download_data = st.session_state.all_people[0]
        download_filename = f"evaluated_{st.session_state.all_people[0].get('person_name', 'unknown').replace(' ', '_')}.json"
    
    download_json = json.dumps(download_data, indent=2, ensure_ascii=False)
    st.download_button(
        label="Download Evaluated File",
        data=download_json,
        file_name=download_filename,
        mime="application/json",
        type="primary"
    )
    
    st.caption("Download includes all your changes")

current_chunk = chunks[chunk_idx]
chunk_id = current_chunk.get("chunk_id")
chunk_text = current_chunk.get("text", "")
person_name = current_chunk.get("person_name", "Unknown")

chunk_events = get_events_for_chunk(chunk_id, all_events)

with st.sidebar:
    st.metric("Events from this chunk", len(chunk_events))
    
    if chunk_events:
        reviewed_count = sum(1 for e in chunk_events if e.get("reviewed"))
        if reviewed_count == len(chunk_events):
            st.success(f"All {len(chunk_events)} events reviewed")
        elif reviewed_count > 0:
            st.warning(f"{reviewed_count}/{len(chunk_events)} events reviewed")
        else:
            st.info("No events reviewed yet")
    
    st.divider()
    
    if st.button("Add Missing Event for This Chunk"):
        new_event = {
            "organization": "",
            "role": "",
            "location": "",
            "start_date": "",
            "end_date": "",
            "description": "",
            "supporting_quotes": [],
            "chunk_id": chunk_id,
            "source_url": current_chunk.get("source_url", "unknown"),
            "manually_added": True,
            "reviewed": False,
            "raw_llm_output": "Manually added by evaluator"
        }
        
        for person_data in st.session_state.all_people:
            if person_data.get("person_name") == person_name:
                person_data["raw_extractions"].append(new_event)
                break
        
        all_events.append(new_event)
        st.rerun()

st.subheader(f"{person_name}")
st.caption(f"Chunk {chunk_idx + 1} of {len(chunks)} | Total events: {len(all_events)}")

left_col, right_col = st.columns([1, 1])

with left_col:
    st.markdown("### Extracted Events")
    st.caption(f"Chunk ID: {chunk_id}")
    
    if not chunk_events:
        st.info("No events extracted from this chunk")
    else:
        events_container = st.container(height=800)
        
        with events_container:
            for event_num, event in enumerate(chunk_events, 1):
                with st.container():
                    st.markdown(f"**Event {event_num}/{len(chunk_events)}**")
                    
                    event_key = f"event_{chunk_idx}_{event_num}_{event.get('chunk_id', 'unk')}"
                    
                    org = st.text_input("Organization", value=event.get("organization", "") or "", key=f"org_{event_key}")
                    role = st.text_input("Role", value=event.get("role", "") or "", key=f"role_{event_key}")
                    location = st.text_input("Location", value=event.get("location", "") or "", key=f"loc_{event_key}")
                    
                    col_start, col_end = st.columns(2)
                    with col_start:
                        start_date = st.text_input("Start", value=event.get("start_date", "") or "", key=f"start_{event_key}")
                    with col_end:
                        end_date = st.text_input("End", value=event.get("end_date", "") or "", key=f"end_{event_key}")
                    
                    description = st.text_area("Description", value=event.get("description", "") or "", height=80, key=f"desc_{event_key}")
                    
                    event["organization"] = org
                    event["role"] = role
                    event["location"] = location
                    event["start_date"] = start_date
                    event["end_date"] = end_date
                    event["description"] = description
                    
                    st.markdown("#### Evaluation")
                    
                    eval_delete = st.checkbox(
                        "Mark for deletion (incorrect/duplicate/noise)", 
                        value=event.get("evaluation_delete", False),
                        key=f"eval_del_{event_key}"
                    )
                    event["evaluation_delete"] = eval_delete
                    
                    eval_narrative = st.text_area(
                        "Evaluation narrative (what's wrong/right with this extraction)",
                        value=event.get("evaluation_narrative", ""),
                        height=100,
                        key=f"eval_narr_{event_key}",
                        placeholder="e.g., 'Missing location', 'Wrong dates', 'Duplicate of event X', 'Perfect extraction'"
                    )
                    event["evaluation_narrative"] = eval_narrative
                    
                    quotes = event.get("supporting_quotes", [])
                    if quotes:
                        st.markdown("**Quotes:**")
                        for i, quote in enumerate(quotes, 1):
                            st.markdown(f"{i}. *\"{quote}\"*")
                    
                    with st.expander("Raw LLM Output"):
                        st.code(event.get("raw_llm_output", "No raw output"), language="json")
                    
                    col_status, col_review = st.columns([2, 1])
                    
                    with col_status:
                        if event.get("manually_added"):
                            st.caption("Manually added")
                        if event.get("evaluation_delete"):
                            st.caption("Marked for deletion")
                        if event.get("reviewed"):
                            st.caption("Reviewed")
                    
                    with col_review:
                        is_reviewed = event.get("reviewed", False)
                        if st.button("Mark Reviewed" if not is_reviewed else "Reviewed", 
                                    key=f"review_{event_key}",
                                    type="primary" if not is_reviewed else "secondary"):
                            if not is_reviewed:
                                event["reviewed"] = True
                                st.rerun()
                    
                    st.markdown("---")

with right_col:
    st.markdown("### Source Chunk Text")
    st.caption(f"Chunk ID: {chunk_id} | Source: {current_chunk.get('source_url', 'unknown')}")
    
    all_quotes = []
    for event in chunk_events:
        all_quotes.extend(event.get("supporting_quotes", []))
    
    if all_quotes:
        highlighted_text = highlight_quotes_in_text(chunk_text, all_quotes)
        st.markdown(
            f'<div style="background-color: white; color: black; padding: 20px; border: 1px solid #ddd; height: 800px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap;">{highlighted_text}</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'<div style="background-color: white; color: black; padding: 20px; border: 1px solid #ddd; height: 800px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap;">{chunk_text}</div>',
            unsafe_allow_html=True
        )

with st.sidebar:
    st.divider()
    st.markdown("### Progress")
    
    total_events = len(all_events)
    reviewed_events = sum(1 for e in all_events if e.get("reviewed"))
    marked_for_deletion = sum(1 for e in all_events if e.get("evaluation_delete"))
    with_narratives = sum(1 for e in all_events if e.get("evaluation_narrative"))
    
    review_progress = reviewed_events / max(total_events, 1) * 100
    
    st.metric("Events reviewed", f"{reviewed_events}/{total_events}")
    st.progress(review_progress / 100)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Marked for deletion", marked_for_deletion)
    with col2:
        st.metric("With narratives", with_narratives)
    
    chunks_with_events = len(set(e.get("chunk_id") for e in all_events if e.get("chunk_id")))
    st.metric("Chunks with events", f"{chunks_with_events}/{len(chunks)}")
    
    manually_added = sum(1 for e in all_events if e.get("manually_added"))
    if manually_added > 0:
        st.metric("Manually added events", manually_added)