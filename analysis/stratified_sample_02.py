import os
import json
import glob
import pandas as pd

path = r'C:\Users\spatt\Desktop\EliteResearchAgent\services\careerfinder_base_01\review'

def extract_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    person_name = data.get('person_name')
    raw_extractions = data.get('raw_extractions', [])

    events = []
    for extraction in raw_extractions:
        chunk_id = extraction.get('chunk_id')
        source_url = extraction.get('source_url')
        raw_llm_output = extraction.get('raw_llm_output', '{}')
        num_events = raw_llm_output.count('"organization"')

        events.append({
            'person_name': person_name,
            'chunk_id': chunk_id,
            'source_url': source_url,
            'num_events': num_events
        })

    return events

json_files = glob.glob(os.path.join(path, 'careerfinder_base_*.json'))

alldata = []
for file in json_files:
    alldata.extend(extract_data(file))

df = pd.DataFrame(alldata)
distribution = df.groupby('chunk_id')['num_events'].sum().reset_index()

bins = [0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000]
labels = ['0', '1', '2', '3', '4', '5-10', '11-20', '21-50', '51-100', '101-200', '201-500', '501-1000']
distribution['range'] = pd.cut(distribution['num_events'], bins=bins, labels=labels, right=False)

range_counts = distribution['range'].value_counts().sort_index()

print("Nombre de chunks par tranche d'événements :")
for label, count in zip(labels, range_counts):
    print(f"{label} : {count}")
