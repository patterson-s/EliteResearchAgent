import streamlit as st
import json
from pathlib import Path
from load_data import get_organization_data, load_config
from fuzzy_grouping import cluster_organizations, get_cluster_summary

def load_ontology():
    config = load_config()
    ontology_file = config['ontology_file']
    
    if Path(ontology_file).exists():
        with open(ontology_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_ontology(ontology):
    config = load_config()
    ontology_file = config['ontology_file']
    
    with open(ontology_file, 'w', encoding='utf-8') as f:
        json.dump(ontology, f, indent=2, ensure_ascii=False)

def get_unmapped_orgs(clusters, ontology):
    unmapped_clusters = []
    
    for cluster in clusters:
        unmapped_in_cluster = [org for org, _ in cluster if org not in ontology]
        if unmapped_in_cluster:
            unmapped_clusters.append([
                (org, count) for org, count in cluster 
                if org in unmapped_in_cluster
            ])
    
    return unmapped_clusters

st.set_page_config(page_title="Organization Ontology Builder", layout="wide")

st.title("Organization Ontology Builder")

if 'ontology' not in st.session_state:
    st.session_state.ontology = load_ontology()

uploaded_file = st.file_uploader("Upload careerfinder JSON file", type=['json'])

if uploaded_file is not None:
    file_content = uploaded_file.read().decode('utf-8')
    
    if 'current_file' not in st.session_state or st.session_state.current_file != uploaded_file.name:
        st.session_state.current_file = uploaded_file.name
        
        org_list, org_examples, person_name = get_organization_data(file_content)
        st.session_state.person_name = person_name
        st.session_state.org_list = org_list
        st.session_state.org_examples = org_examples
        
        config = load_config()
        clusters = cluster_organizations(org_list, config['fuzzy_threshold'])
        st.session_state.clusters = clusters
    
    st.info(f"Reviewing career events for: **{st.session_state.person_name}**")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Statistics")
        total_orgs = len(st.session_state.org_list)
        mapped_orgs = sum(1 for org, _ in st.session_state.org_list if org in st.session_state.ontology)
        unmapped_orgs = total_orgs - mapped_orgs
        
        st.metric("Total Organizations", total_orgs)
        st.metric("Mapped", mapped_orgs)
        st.metric("Unmapped", unmapped_orgs)
        
        if total_orgs > 0:
            progress = mapped_orgs / total_orgs
            st.progress(progress)
        
        st.divider()
        
        filter_option = st.radio(
            "Show clusters",
            ["All clusters", "Only unmapped", "Only mapped"]
        )
        
        if filter_option == "Only unmapped":
            display_clusters = get_unmapped_orgs(st.session_state.clusters, st.session_state.ontology)
        elif filter_option == "Only mapped":
            display_clusters = [
                cluster for cluster in st.session_state.clusters
                if any(org in st.session_state.ontology for org, _ in cluster)
            ]
        else:
            display_clusters = st.session_state.clusters
        
        st.write(f"Showing {len(display_clusters)} clusters")
        
        cluster_options = []
        for i, cluster in enumerate(display_clusters):
            summary = get_cluster_summary(cluster)
            mapped_count = sum(1 for org, _ in cluster if org in st.session_state.ontology)
            status = f"[{mapped_count}/{summary['size']} mapped]"
            label = f"{status} {summary['primary']} ({summary['total_mentions']} mentions)"
            cluster_options.append(label)
        
        if cluster_options:
            selected_idx = st.selectbox(
                "Select cluster to review",
                range(len(display_clusters)),
                format_func=lambda i: cluster_options[i]
            )
        else:
            selected_idx = None
    
    with col2:
        if selected_idx is not None:
            selected_cluster = display_clusters[selected_idx]
            
            st.subheader("Cluster Details")
            
            for org, count in selected_cluster:
                is_mapped = org in st.session_state.ontology
                
                with st.expander(f"{'✓' if is_mapped else '○'} {org} ({count} mentions)", expanded=not is_mapped):
                    if is_mapped:
                        st.json(st.session_state.ontology[org])
                        if st.button(f"Remove mapping", key=f"remove_{org}"):
                            del st.session_state.ontology[org]
                            save_ontology(st.session_state.ontology)
                            st.rerun()
                    else:
                        examples = st.session_state.org_examples.get(org, [])[:3]
                        
                        if examples:
                            st.write("Example contexts:")
                            for ex in examples:
                                st.text(f"- {ex['person']}: {ex['role']} at {ex['location']} ({ex['dates']})")
            
            st.divider()
            st.subheader("Create Canonical Entity")
            
            selected_orgs = st.multiselect(
                "Select organizations to map",
                [org for org, _ in selected_cluster if org not in st.session_state.ontology],
                default=[org for org, _ in selected_cluster if org not in st.session_state.ontology][:1]
            )
            
            if selected_orgs:
                if 'entity_fields' not in st.session_state:
                    st.session_state.entity_fields = [
                        {'key': 'canonical_name', 'value': selected_orgs[0]},
                        {'key': 'org_type', 'value': ''},
                        {'key': 'country', 'value': ''}
                    ]
                
                st.write("**Entity Attributes**")
                
                fields_to_remove = []
                for i, field in enumerate(st.session_state.entity_fields):
                    col_key, col_val, col_del = st.columns([2, 3, 1])
                    
                    with col_key:
                        new_key = st.text_input(
                            "Field name",
                            value=field['key'],
                            key=f"key_{i}",
                            label_visibility="collapsed"
                        )
                        st.session_state.entity_fields[i]['key'] = new_key
                    
                    with col_val:
                        new_val = st.text_input(
                            "Value",
                            value=field['value'],
                            key=f"val_{i}",
                            label_visibility="collapsed"
                        )
                        st.session_state.entity_fields[i]['value'] = new_val
                    
                    with col_del:
                        if st.button("✕", key=f"del_{i}"):
                            fields_to_remove.append(i)
                
                for idx in reversed(fields_to_remove):
                    st.session_state.entity_fields.pop(idx)
                
                col_add, col_space = st.columns([1, 3])
                with col_add:
                    if st.button("➕ Add Field"):
                        st.session_state.entity_fields.append({'key': '', 'value': ''})
                        st.rerun()
                
                st.divider()
                
                col_common, col_hierarchy = st.columns(2)
                
                with col_common:
                    st.write("**Quick Add Common Fields**")
                    if st.button("Add Location"):
                        st.session_state.entity_fields.append({'key': 'location', 'value': ''})
                        st.rerun()
                    if st.button("Add Parent Org"):
                        st.session_state.entity_fields.append({'key': 'parent_org', 'value': ''})
                        st.rerun()
                    if st.button("Add Founding Year"):
                        st.session_state.entity_fields.append({'key': 'founded', 'value': ''})
                        st.rerun()
                
                with col_hierarchy:
                    st.write("**Hierarchy Helper**")
                    existing_orgs = list(set(
                        entity.get('canonical_name', '')
                        for entity in st.session_state.ontology.values()
                        if entity.get('canonical_name')
                    ))
                    
                    if existing_orgs:
                        parent_select = st.selectbox(
                            "Set parent from existing orgs",
                            [''] + sorted(existing_orgs),
                            key="parent_helper"
                        )
                        if parent_select and st.button("Apply Parent"):
                            parent_exists = False
                            for field in st.session_state.entity_fields:
                                if field['key'] == 'parent_org':
                                    field['value'] = parent_select
                                    parent_exists = True
                                    break
                            if not parent_exists:
                                st.session_state.entity_fields.append({'key': 'parent_org', 'value': parent_select})
                            st.rerun()
                
                st.divider()
                
                col_save, col_preview, col_cancel = st.columns(3)
                
                with col_preview:
                    if st.button("Preview JSON"):
                        entity = {
                            field['key']: field['value']
                            for field in st.session_state.entity_fields
                            if field['key'] and field['value']
                        }
                        st.json(entity)
                
                with col_save:
                    if st.button("💾 Save Mapping"):
                        entity = {
                            field['key']: field['value']
                            for field in st.session_state.entity_fields
                            if field['key'] and field['value']
                        }
                        
                        if not entity:
                            st.error("Entity must have at least one field")
                        else:
                            for org in selected_orgs:
                                st.session_state.ontology[org] = entity
                            
                            save_ontology(st.session_state.ontology)
                            st.success(f"Mapped {len(selected_orgs)} organization(s)")
                            
                            st.session_state.entity_fields = [
                                {'key': 'canonical_name', 'value': ''},
                                {'key': 'org_type', 'value': ''},
                                {'key': 'country', 'value': ''}
                            ]
                            st.rerun()
                
                with col_cancel:
                    if st.button("Reset Form"):
                        st.session_state.entity_fields = [
                            {'key': 'canonical_name', 'value': selected_orgs[0] if selected_orgs else ''},
                            {'key': 'org_type', 'value': ''},
                            {'key': 'country', 'value': ''}
                        ]
                        st.rerun()
else:
    st.info("Upload a careerfinder JSON file to begin")

st.sidebar.title("Actions")
if st.sidebar.button("Clear File"):
    if 'current_file' in st.session_state:
        del st.session_state.current_file
    st.rerun()

if st.sidebar.button("Export Ontology"):
    st.sidebar.download_button(
        "Download JSON",
        json.dumps(st.session_state.ontology, indent=2, ensure_ascii=False),
        "ontology.json",
        "application/json"
    )