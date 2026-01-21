import os
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt

# Chemin vers les fichiers JSON
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

# Charger tous les fichiers JSON
json_files = glob.glob(os.path.join(path, 'careerfinder_base_*.json'))

# Extraire les données de chaque fichier
alldata = []
for file in json_files:
    alldata.extend(extract_data(file))

# Créer un DataFrame pour analyse
df = pd.DataFrame(alldata)

# Distribution du nombre d'événements par chunk
distribution = df.groupby('chunk_id')['num_events'].sum().reset_index()

# Statistiques
distribution_stats = {
    'min': distribution['num_events'].min(),
    'max': distribution['num_events'].max(),
    'mean': distribution['num_events'].mean(),
    'median': distribution['num_events'].median(),
    'std': distribution['num_events'].std(),
}

print(f"Nombre total de personnes uniques : {df['person_name'].nunique()}")
print(f"Nombre total d'URL sources uniques : {df['source_url'].nunique()}")
print(f"Nombre total de chunk_id uniques : {df['chunk_id'].nunique()}")
print("Distribution du nombre d'événements par chunk :")
for stat, value in distribution_stats.items():
    print(f"{stat} : {value}")

# Tracer l'histogramme et la densité
plt.figure(figsize=(10, 6))
plt.hist(distribution['num_events'], bins=30, density=True, alpha=0.6, color='g')
distribution['num_events'].plot(kind='density', color='r')
plt.title('Distribution du nombre d\'événements par chunk')
plt.xlabel('Nombre d\'événements')
plt.ylabel('Densité')
plt.grid(True)
plt.savefig('distribution_events_per_chunk.png')
plt.close()

print("Un graphique de la distribution a été sauvegardé sous 'distribution_events_per_chunk.png'.")
