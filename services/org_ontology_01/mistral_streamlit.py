import json
import pandas as pd
import streamlit as st
import os

def charger_donnees(fichiers):
    donnees = []
    for fichier in fichiers:
        try:
            contenu = json.load(fichier)
            for extraction in contenu.get("raw_extractions", []):
                donnees.append({
                    "person_name": contenu["person_name"],
                    "organization": extraction["organization"],
                    "role": extraction["role"],
                    "location": extraction["location"],
                    "start_date": extraction["start_date"],
                    "end_date": extraction["end_date"],
                    "description": extraction["description"],
                    "supporting_quotes": extraction["supporting_quotes"],
                    "fichier": fichier.name
                })
        except Exception as e:
            st.error(f"Erreur lors du chargement du fichier {fichier.name}: {e}")
    return pd.DataFrame(donnees)

def gerer_motifs():
    st.subheader("Gestion des motifs")

    motifs_sauvegardes = [f.replace('.motif.json', '') for f in os.listdir() if f.endswith('.motif.json')]
    motif_selectionne = st.selectbox("Motifs sauvegardés", ["Nouveau motif"] + motifs_sauvegardes)

    if motif_selectionne != "Nouveau motif":
        with open(f"{motif_selectionne}.motif.json", 'r', encoding='utf-8') as f:
            motif = json.load(f)
    else:
        motif = {"Type": "", "Details": {}}

    st.write("### Éditeur de motif")
    if "motif_edition" not in st.session_state:
        st.session_state.motif_edition = motif

    motif = st.session_state.motif_edition

    if isinstance(motif, dict):
        keys = list(motif.keys())
        for key in keys:
            col1, col2, col3, col4 = st.columns([2, 4, 1, 1])
            with col1:
                new_key = st.text_input(f"Clé", value=key, key=f"key_{key}")
            with col2:
                value = motif[key]
                if isinstance(value, dict):
                    st.text_input(f"Valeur", value=f"{{...}}", key=f"value_{key}", disabled=True)
                else:
                    new_value = st.text_input(f"Valeur", value=str(value) if value is not None else "", key=f"value_{key}")

                if isinstance(value, dict):
                    if st.button(f"Désimbriquer", key=f"unnest_{key}"):
                        st.session_state.motif_edition[key] = json.dumps(value)
            with col3:
                if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                    if st.button(f"Imbriquer", key=f"nest_{key}"):
                        try:
                            st.session_state.motif_edition[key] = json.loads(value)
                        except json.JSONDecodeError:
                            pass
            with col4:
                if st.button(f"Supprimer", key=f"delete_{key}"):
                    del st.session_state.motif_edition[key]
                    st.rerun()

    if st.button("Ajouter une clé"):
        new_key = st.text_input("Nouvelle clé")
        new_value = st.text_input("Nouvelle valeur")
        if new_key and new_key not in st.session_state.motif_edition:
            st.session_state.motif_edition[new_key] = new_value
            st.rerun()

    nom_motif = st.text_input("Nom du motif pour sauvegarde")
    if st.button("Enregistrer le motif"):
        if nom_motif:
            with open(f"{nom_motif}.motif.json", 'w', encoding='utf-8') as f:
                json.dump(st.session_state.motif_edition, f, indent=2, ensure_ascii=False)
            st.success(f"Motif sauvegardé sous {nom_motif}.motif.json")
        else:
            st.error("Veuillez entrer un nom pour le motif.")

    if st.button("Charger le motif"):
        st.session_state.motif = st.session_state.motif_edition
        st.write("Motif chargé.")

def afficher_et_associer_par_entree(df):
    st.subheader("Parcourir et associer les entrées")

    if "index_actuel" not in st.session_state:
        st.session_state.index_actuel = 0
    if "correspondances" not in st.session_state:
        st.session_state.correspondances = []

    index = st.session_state.index_actuel
    if index < len(df):
        row = df.iloc[index]
        st.markdown(f"---")
        st.write(f"**Entrée {index + 1} / {len(df)}**")
        st.write(f"Organisation: {row['organization']}")
        st.write(f"Rôle: {row['role']}")
        st.write(f"Description: {row['description']}")
        st.write(f"Citation: {row['supporting_quotes']}")

        if "motif" in st.session_state:
            st.write("Motif actuel :", json.dumps(st.session_state.motif, indent=2))

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("Associer", key=f"associer_{index}"):
                st.session_state.correspondances.append({
                    "événement": row.to_dict(),
                    "motif": st.session_state.motif
                })
                st.success("Entrée associée.")

        with col2:
            if st.button("Ignorer", key=f"ignorer_{index}"):
                st.warning("Entrée ignorée.")

        with col3:
            if st.button("Précédent", key=f"precedent_{index}") and index > 0:
                st.session_state.index_actuel -= 1
                st.rerun()

        with col4:
            if st.button("Suivant", key=f"suivant_{index}"):
                st.session_state.index_actuel += 1
                st.rerun()

    if st.session_state.index_actuel >= len(df):
        st.write("Toutes les entrées ont été parcourues.")

def sauvegarder_resultats():
    st.subheader("Sauvegarder les résultats")
    if "correspondances" not in st.session_state or not st.session_state.correspondances:
        st.warning("Aucune correspondance à sauvegarder.")
        return

    nom_fichier = st.text_input("Nom du fichier de sauvegarde", "correspondances.json")
    if st.button("Sauvegarder"):
        with open(nom_fichier, 'w', encoding='utf-8') as f:
            json.dump(st.session_state.correspondances, f, indent=4, ensure_ascii=False)
        st.success(f"Correspondances sauvegardées dans {nom_fichier}.")

def main():
    st.title("Outil de correspondance pour ontologie organisationnelle")

    fichiers = st.file_uploader("Sélectionnez les fichiers JSON", type=["json"], accept_multiple_files=True)

    if fichiers:
        df = charger_donnees(fichiers)
        st.session_state.df = df
        st.write(f"Données chargées : {len(df)} événements.")

    if "df" in st.session_state:
        gerer_motifs()
        afficher_et_associer_par_entree(st.session_state.df)
        sauvegarder_resultats()

if __name__ == "__main__":
    main()
