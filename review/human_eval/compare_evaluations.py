import json
import pandas as pd
from pathlib import Path

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def extract_raw_extractions(data):
    return data.get('raw_extractions', [])

def compare_evaluations(file1_path, file2_path):
    # Charger les données
    data1 = load_json(file1_path)
    data2 = load_json(file2_path)

    # Extraire les extractions brutes
    extractions1 = extract_raw_extractions(data1)
    extractions2 = extract_raw_extractions(data2)

    # Convertir en DataFrames pour une meilleure visualisation
    df1 = pd.DataFrame(extractions1)
    df2 = pd.DataFrame(extractions2)

    # Afficher les colonnes disponibles
    print("Colonnes dans le premier fichier:", df1.columns.tolist())
    print("Colonnes dans le deuxième fichier:", df2.columns.tolist())

    # Afficher les premières lignes de chaque DataFrame
    print("\nPremières lignes du premier fichier:")
    print(df1.head())

    print("\nPremières lignes du deuxième fichier:")
    print(df2.head())

    # Comparer les organisations et rôles
    print("\nComparaison des organisations et rôles:")
    for i, (ext1, ext2) in enumerate(zip(extractions1, extractions2)):
        print(f"\nEntrée {i+1}:")
        print(f"Fichier 1 - Organisation: {ext1.get('organization')}, Rôle: {ext1.get('role')}")
        print(f"Fichier 2 - Organisation: {ext2.get('organization')}, Rôle: {ext2.get('role')}")

    return df1, df2

if __name__ == "__main__":
    # Chemins vers les fichiers
    file1_path = Path("A. Banerjee (chunks 1 to 6).json")
    file2_path = Path("evaluated_Abhijit_Banerjee.json")

    # Comparer les évaluations
    df1, df2 = compare_evaluations(file1_path, file2_path)
