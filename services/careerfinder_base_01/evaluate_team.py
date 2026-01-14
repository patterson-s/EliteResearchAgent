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

def initialize_session_state():
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    if "person_data" not in st.session_state:
        st.session_state.person_data = None
    if "chunks_data" not in st.session_state:
        st.session_state.chunks_data = None
    if "current_chunk_idx" not in st.session_state:
        st.session_state.current_chunk_idx = 0

initialize_session_state()

st.title("Career Event Evaluator - Team Version")

if not st.session_state.data_loaded:
    st.markdown("### Upload Files to Begin")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Person Result File")
        person_file = st.file_uploader(
            "Upload person result JSON",
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
                person_data = json.load(person_file)
                all_chunks = json.load(chunks_file)
                
                person_name = person_data.get("person_name")
                if not person_name:
                    st.error("Person result file missing 'person_name' field")
                    st.stop()
                
                person_chunks = [c for c in all_chunks if c.get("person_name") == person_name]
                
                if not person_chunks:
                    st.error(f"No chunks found for {person_name} in chunks file")
                    st.stop()
                
                if "evaluation_metadata" not in person_data:
                    person_data["evaluation_metadata"] = {
                        "evaluator": "",
                        "evaluator_note": ""
                    }
                
                st.session_state.person_data = person_data
                st.session_state.chunks_data = person_chunks
                st.session_state.data_loaded = True
                st.session_state.current_chunk_idx = 0
                
                st.success(f"Loaded {len(person_chunks)} chunks for {person_name}")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error loading files: {e}")
    
    st.stop()

data = st.session_state.person_data
chunks = st.session_state.chunks_data
person_name = data.get("person_name", "Unknown")
events = data.get("raw_extractions", [])
evaluation_metadata = data["evaluation_metadata"]

with st.sidebar:
    st.header("Evaluator Info")
    
    evaluator = st.text_input("Evaluator name", value=evaluation_metadata.get("evaluator", ""))
    evaluator_note = st.text_area("Evaluation note", value=evaluation_metadata.get("evaluator_note", ""), height=100)
    
    evaluation_metadata["evaluator"] = evaluator
    evaluation_metadata["evaluator_note"] = evaluator_note
    
    st.divider()
    st.subheader("Chunk Navigation")
    
    chunk_idx = st.number_input(
        "Chunk number",
        min_value=1,
        max_value=len(chunks),
        value=st.session_state.current_chunk_idx + 1,
        step=1,
        key="chunk_nav"
    ) - 1
    
    if chunk_idx != st.session_state.current_chunk_idx:
        st.session_state.current_chunk_idx = chunk_idx
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
    
    download_json = json.dumps(data, indent=2, ensure_ascii=False)
    st.download_button(
        label="Download Evaluated File",
        data=download_json,
        file_name=f"evaluated_{person_name.replace(' ', '_')}.json",
        mime="application/json",
        type="primary"
    )
    
    st.caption("Download includes all your changes")

current_chunk = chunks[chunk_idx]
chunk_id = current_chunk.get("chunk_id")
chunk_text = current_chunk.get("text", "")

chunk_events = get_events_for_chunk(chunk_id, events)

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
        data["raw_extractions"].append(new_event)
        events.append(new_event)
        st.rerun()

st.subheader(f"{person_name}")
st.caption(f"Total chunks: {len(chunks)} | Total events extracted: {len(events)}")

left_col, right_col = st.columns([1, 1])

with left_col:
    st.markdown("### Extracted Events")
    st.caption(f"Chunk {chunk_idx + 1} of {len(chunks)} | Chunk ID: {chunk_id}")
    
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
    
    total_events = len(events)
    reviewed_events = sum(1 for e in events if e.get("reviewed"))
    marked_for_deletion = sum(1 for e in events if e.get("evaluation_delete"))
    with_narratives = sum(1 for e in events if e.get("evaluation_narrative"))
    
    review_progress = reviewed_events / max(total_events, 1) * 100
    
    st.metric("Events reviewed", f"{reviewed_events}/{total_events}")
    st.progress(review_progress / 100)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Marked for deletion", marked_for_deletion)
    with col2:
        st.metric("With narratives", with_narratives)
    
    chunks_with_events = len(set(e.get("chunk_id") for e in events if e.get("chunk_id")))
    st.metric("Chunks with events", f"{chunks_with_events}/{len(chunks)}")
    
    manually_added = sum(1 for e in events if e.get("manually_added"))
    if manually_added > 0:
        st.metric("Manually added events", manually_added)