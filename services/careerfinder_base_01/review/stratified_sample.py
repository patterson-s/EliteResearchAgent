import os
import json
import glob
import pandas as pd
from datetime import datetime

path = r'C:\Users\spatt\Desktop\EliteResearchAgent\services\careerfinder_base_01\review'

def extract_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    person_name = data.get('person_name')
    timestamp = data.get('timestamp')
    config = data.get('config')
    raw_extractions = data.get('raw_extractions', [])

    events = []
    for extraction in raw_extractions:
        chunk_id = extraction.get('chunk_id')
        source_url = extraction.get('source_url')
        raw_llm_output = extraction.get('raw_llm_output', '{}')
        num_events = raw_llm_output.count('"organization"')

        events.append({
            'person_name': person_name,
            'timestamp': timestamp,
            'config': config,
            'chunk_id': chunk_id,
            'source_url': source_url,
            'num_events': num_events,
            'extraction': extraction
        })

    return events

json_files = glob.glob(os.path.join(path, 'careerfinder_base_*.json'))

alldata = []
for file in json_files:
    alldata.extend(extract_data(file))

df = pd.DataFrame(alldata)
distribution = df.groupby('chunk_id')['num_events'].sum().reset_index()

# Filtrer les chunks avec 1 à 20 événements
filtered_distribution = distribution[(distribution['num_events'] >= 1) & (distribution['num_events'] <= 20)]

# Séparer les tranches
bin_1_10 = filtered_distribution[(filtered_distribution['num_events'] >= 1) & (filtered_distribution['num_events'] <= 10)]
bin_11_20 = filtered_distribution[(filtered_distribution['num_events'] >= 11) & (filtered_distribution['num_events'] <= 20)]

# Échantillonner 35 chunks de la tranche 1-10 et 15 chunks de la tranche 11-20 pour les chunks communs
common_chunks_1_10 = bin_1_10.sample(n=35, random_state=42)
common_chunks_11_20 = bin_11_20.sample(n=15, random_state=42)
common_chunks = pd.concat([common_chunks_1_10, common_chunks_11_20])

# Échantillonner 35 chunks de la tranche 1-10 et 15 chunks de la tranche 11-20 pour les chunks uniques du relecteur 1
remaining_1_10 = bin_1_10.drop(common_chunks_1_10.index)
remaining_11_20 = bin_11_20.drop(common_chunks_11_20.index)
unique_chunks_reviewer1_1_10 = remaining_1_10.sample(n=35, random_state=42)
unique_chunks_reviewer1_11_20 = remaining_11_20.sample(n=15, random_state=42)
unique_chunks_reviewer1 = pd.concat([unique_chunks_reviewer1_1_10, unique_chunks_reviewer1_11_20])

# Échantillonner 35 chunks de la tranche 1-10 et 15 chunks de la tranche 11-20 pour les chunks uniques du relecteur 2
remaining_1_10 = remaining_1_10.drop(unique_chunks_reviewer1_1_10.index)
remaining_11_20 = remaining_11_20.drop(unique_chunks_reviewer1_11_20.index)
unique_chunks_reviewer2_1_10 = remaining_1_10.sample(n=35, random_state=42)
unique_chunks_reviewer2_11_20 = remaining_11_20.sample(n=15, random_state=42)
unique_chunks_reviewer2 = pd.concat([unique_chunks_reviewer2_1_10, unique_chunks_reviewer2_11_20])

# Combiner les chunks pour chaque relecteur
reviewer1_chunk_ids = pd.concat([common_chunks, unique_chunks_reviewer1])['chunk_id'].tolist()
reviewer2_chunk_ids = pd.concat([common_chunks, unique_chunks_reviewer2])['chunk_id'].tolist()

def create_evaluation_file(chunk_ids, filename):
    # Regrouper les chunks par personne
    reviewer_data = []
    for chunk_id in chunk_ids:
        chunk_info = df[df['chunk_id'] == chunk_id].iloc[0]
        person_name = chunk_info['person_name']

        # Vérifier si la personne existe déjà dans reviewer_data
        person_entry = next((entry for entry in reviewer_data if entry['person_name'] == person_name), None)
        if person_entry is None:
            person_entry = {
                "person_name": person_name,
                "timestamp": chunk_info['timestamp'],
                "config": chunk_info['config'],
                "raw_extractions": []
            }
            reviewer_data.append(person_entry)

        # Ajouter le chunk à la liste des extractions de cette personne
        person_entry["raw_extractions"].append(chunk_info['extraction'])

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(reviewer_data, f, ensure_ascii=False, indent=2)

create_evaluation_file(reviewer1_chunk_ids, 'reviewer1_evaluation.json')
create_evaluation_file(reviewer2_chunk_ids, 'reviewer2_evaluation.json')
