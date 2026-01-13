import json
import streamlit as st
from pathlib import Path
from typing import List, Dict, Any
import re

st.set_page_config(page_title="Career Event Evaluator", layout="wide")

def load_result_file(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_result_file(data: Dict[str, Any], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_chunks_data(chunks_file: Path, person_name: str) -> List[Dict[str, Any]]:
    with open(chunks_file, "r", encoding="utf-8") as f:
        all_chunks = json.load(f)
    
    person_chunks = [c for c in all_chunks if c.get("person_name") == person_name]
    return person_chunks

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

st.title("Career Event Evaluator - Chunk-by-Chunk Recall Testing")

with st.sidebar:
    st.header("Configuration")
    
    review_dir = st.text_input(
        "Review directory",
        value=str(Path("review"))
    )
    
    chunks_file = st.text_input(
        "Chunks file",
        value=str(Path("data/all_chunks.json"))
    )
    
    review_path = Path(review_dir)
    if not review_path.exists():
        st.error(f"Review directory not found: {review_dir}")
        st.stop()
    
    result_files = sorted(review_path.glob("careerfinder_base_*.json"))
    
    if not result_files:
        st.warning("No result files found")
        st.stop()
    
    selected_file = st.selectbox(
        "Select result file",
        options=result_files,
        format_func=lambda x: x.name
    )

data = load_result_file(selected_file)
person_name = data.get("person_name", "Unknown")
events = data.get("raw_extractions", [])

if "evaluation_metadata" not in data:
    data["evaluation_metadata"] = {
        "evaluator": "",
        "evaluator_note": ""
    }

evaluation_metadata = data["evaluation_metadata"]

with st.sidebar:
    st.divider()
    st.subheader("Evaluator Info")
    
    evaluator = st.text_input("Evaluator name", value=evaluation_metadata.get("evaluator", ""))
    evaluator_note = st.text_area("Evaluation note", value=evaluation_metadata.get("evaluator_note", ""), height=100)
    
    if evaluator != evaluation_metadata.get("evaluator", "") or evaluator_note != evaluation_metadata.get("evaluator_note", ""):
        evaluation_metadata["evaluator"] = evaluator
        evaluation_metadata["evaluator_note"] = evaluator_note
        if st.button("💾 Save Evaluator Info"):
            save_result_file(data, selected_file)
            st.success("Saved!")

chunks_file_path = Path(chunks_file)
if not chunks_file_path.exists():
    st.error(f"Chunks file not found: {chunks_file}")
    st.stop()

chunks = load_chunks_data(chunks_file_path, person_name)

if not chunks:
    st.error(f"No chunks found for {person_name}")
    st.stop()

st.subheader(f"{person_name}")
st.caption(f"Total chunks: {len(chunks)} | Total events extracted: {len(events)}")

with st.sidebar:
    st.divider()
    st.subheader("Chunk Navigation")
    
    chunk_idx = st.number_input(
        "Chunk number",
        min_value=1,
        max_value=len(chunks),
        value=1,
        step=1
    ) - 1
    
    col_prev, col_next = st.columns(2)
    with col_prev:
        if st.button("← Previous", disabled=(chunk_idx == 0)):
            st.rerun()
    with col_next:
        if st.button("Next →", disabled=(chunk_idx == len(chunks) - 1)):
            st.rerun()
    
    st.divider()
    
    if st.button("💾 Save All Changes"):
        save_result_file(data, selected_file)
        st.success("All changes saved!")
        st.rerun()
    
    st.caption("Changes auto-save when you mark events as reviewed")

current_chunk = chunks[chunk_idx]
chunk_id = current_chunk.get("chunk_id")
chunk_text = current_chunk.get("text", "")

chunk_events = get_events_for_chunk(chunk_id, events)

with st.sidebar:
    st.metric("Events from this chunk", len(chunk_events))
    
    if chunk_events:
        reviewed_count = sum(1 for e in chunk_events if e.get("reviewed"))
        if reviewed_count == len(chunk_events):
            st.success(f"✓ All {len(chunk_events)} events reviewed")
        elif reviewed_count > 0:
            st.warning(f"{reviewed_count}/{len(chunk_events)} events reviewed")
        else:
            st.info("No events reviewed yet")
    
    st.divider()
    
    if st.button("➕ Add Missing Event for This Chunk"):
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
        save_result_file(data, selected_file)
        st.success("Added blank event")
        st.rerun()

left_col, right_col = st.columns([1, 1])

with left_col:
    st.markdown("### Extracted Events")
    st.caption(f"Chunk {chunk_idx + 1} of {len(chunks)} | Chunk ID: {chunk_id}")
    
    if not chunk_events:
        st.info("No events extracted from this chunk")
    else:
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
                        st.caption("➕ Manually added")
                    if event.get("evaluation_delete"):
                        st.caption("🗑️ Marked for deletion")
                    if event.get("reviewed"):
                        st.caption("✓ Reviewed")
                
                with col_review:
                    is_reviewed = event.get("reviewed", False)
                    if st.button("✓ Mark Reviewed" if not is_reviewed else "✓ Reviewed", 
                                key=f"review_{event_key}",
                                type="primary" if not is_reviewed else "secondary"):
                        if not is_reviewed:
                            event["reviewed"] = True
                            save_result_file(data, selected_file)
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