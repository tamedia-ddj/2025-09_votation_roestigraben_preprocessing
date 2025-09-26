#!/usr/bin/env python3
"""
Script "backup" pour création des profils intercity des votations. 
SANS HARMONISATION DES COMMUNES.
"""

import json
import csv
import requests
import pandas as pd

# =============================================================================
# CONFIGURATION - ETAPES
# 1. ENTRER "BFS_URL" JSON DU JOUR DE VOTATION
# 2. ENTRER "VOTING_DATE" JOUR DE VOTATION (YYYY-MM-DD)
# 3. ENTRER "INTERCITY_FILE", LE FICHIER INTERCITY HARMONISÉ
# (4. EVENTUELLEMENT CHANGER LES NOMS DES EXPORTS)
# 5. LANCER LE SCRIPT

# Les fichiers générés sont:
# 1. CSV des résultats des communes pour toutes les votations du jour :
# - output/df_votes_<VOTING_DATE>_municipalities.csv
# 2. CSV des des profils intercity (avec les résultats des votations):
# - output/backup_profil_results_<VOTING_DATE>.csv
# 3. CSV des noms des votations dans les 2 langues:
# - output/backup_ballot_names_<VOTING_DATE>.csv
# =============================================================================

# Données de base des votations
# Aller chercher l'URL du JSON de la votation sur:
# https://opendata.swiss/de/dataset/echtzeitdaten-am-abstimmungstag-zu-eidgenoessischen-abstimmungsvorlagen

# -----------------------------------------------------------------------------
# Variables de configuration
BFS_URL = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/7686135/master"
VOTING_DATE = "2000-09-24" # YYYY-MM-DD
INTERCITY_FILE = "input/processed/intercity_harmonized_2025-09-24.csv" # j'ai repris le fichier délà harmonisé pour 2025-09-24

OUTPUT_COMMUNES_FILE = f"output/df_votes_{VOTING_DATE}_municipalities.csv"
OUTPUT_PROFILS_FILE = f"output/backup_profil_results_{VOTING_DATE}.csv"
BALLOTS_INFO_FILE = f"output/backup_ballot_names_{VOTING_DATE}.csv"

# -----------------------------------------------------------------------------

def fetch_bfs_results():
    """Récupère les données depuis l'API BFS"""
    try:
        response = requests.get(BFS_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None

def extract_commune_results(data):
    """Extrait les résultats par commune"""
    results = []
    
    for ballot in data['schweiz']['vorlagen']:
        ballot_name = next(
            (title['text'] for title in ballot['vorlagenTitel'] if title['langKey'] == 'fr'),
            "Titre non disponible"
        )
        
        for canton in ballot['kantone']:
            for commune in canton['gemeinden']:
                results.append({
                    'id': commune['geoLevelnummer'],
                    'commune_name': commune['geoLevelname'],
                    'canton_id': canton['geoLevelnummer'],
                    'canton_name': canton['geoLevelname'],
                    'ballot_id': ballot['vorlagenId'],
                    'ballot_name': ballot_name,
                    'yes_votes': commune['resultat']['jaStimmenAbsolut'],
                    'no_votes': commune['resultat']['neinStimmenAbsolut'],
                    'yes_pct': commune['resultat']['jaStimmenInProzent'],
                    'turnout_pct': commune['resultat']['stimmbeteiligungInProzent'],
                    'ballots_returned': commune['resultat']['eingelegteStimmzettel'],
                    'eligible_voters': commune['resultat']['anzahlStimmberechtigte'],
                    'valid_votes': commune['resultat']['gueltigeStimmen']
                })
    
    return results

def extract_ballot_names(data):
    """Extrait les noms des ballots en format long avec une ligne par langue (FR/DE)"""
    print("Extraction des noms de votations...")
    
    ballots_info = []
    
    for ballot in data['schweiz']['vorlagen']:
        ballot_id = ballot['vorlagenId']
        
        # Extraire les titres en français et allemand
        for title in ballot['vorlagenTitel']:
            if title['langKey'] in ['fr', 'de']:
                ballots_info.append({
                    'ballot-id': ballot_id,
                    'langue': title['langKey'].upper(),  # FR ou DE
                    'title_long': title['text'],
                    'title_short': ''  # À remplir manuellement
                })
    
    df_ballots = pd.DataFrame(ballots_info)
    
    # Trier par ballot_id puis par langue pour avoir un ordre cohérent
    df_ballots = df_ballots.sort_values(['ballot-id', 'langue'])
    
    # Export
    df_ballots.to_csv(BALLOTS_INFO_FILE, index=False)
    print(f"Noms des votations exportés vers {BALLOTS_INFO_FILE} ({len(df_ballots)} lignes)")
    
    return df_ballots

def write_csv(data, filename):
    """Écrit les données dans un fichier CSV"""
    if not data:
        print("Aucune donnée à écrire")
        return
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Données exportées vers {filename} ({len(data)} lignes)")

def process_votation_results():
    """Traite les résultats de votation et génère le fichier de profils"""
    
    # Chargement des données
    print("Chargement des résultats de votation...")
    results_votation_polg = pd.read_csv(OUTPUT_COMMUNES_FILE)
    
    print("Chargement des données intercity...")
    intercity_combined_polg_df = pd.read_csv(INTERCITY_FILE)
    
    # Split par ballot_id (équivalent du split R)
    print("Séparation par objet de votation...")
    results_votation_for_export_split = {}
    for ballot_id in results_votation_polg['ballot_id'].unique():
        df_name = f"results_{ballot_id}"
        results_votation_for_export_split[df_name] = results_votation_polg[
            results_votation_polg['ballot_id'] == ballot_id
        ]
        print(f"Créé: {df_name} avec {len(results_votation_for_export_split[df_name])} lignes")
    
    # Merge avec intercity (left join)
    print("Fusion avec les données intercity...")
    results_votation_for_export_merge_results = pd.merge(
        intercity_combined_polg_df,
        results_votation_polg[['id', 'ballot_id', 'yes_pct']],  # Seulement les colonnes nécessaires
        left_on='GMDNR',
        right_on='id',
        how='left'
    )
    
    # Renommage des colonnes pour correspondre à la structure attendue
    results_final = results_votation_for_export_merge_results.rename(columns={
        'order_ic': 'order',
        'Name_fr': 'GMDNAME',
        'fr': 'GMDNAME_FR',
        'de': 'GMDNAME_DE',
        'iso2': 'KTN_abr'
    })
    
    # Sélection et ordre des colonnes finales
    columns_order = ['GMDNR', 'order', 'ligne', 'GMDNAME', 'GMDNAME_FR', 'GMDNAME_DE', 'KTN_abr', 'ballot_id', 'yes_pct']
    results_final = results_final[columns_order]
    
    # Tri et export final
    print("Tri et export final...")
    results_final = results_final.sort_values(['ballot_id', 'ligne', 'order'])
    
    results_final.to_csv(OUTPUT_PROFILS_FILE, index=False)
    print(f"Profils exportés vers {OUTPUT_PROFILS_FILE} ({len(results_final)} lignes)")

# Exécution du script
if __name__ == "__main__":
    print("=== Récupération des données BFS ===")
    bfs_data = fetch_bfs_results()
    
    if bfs_data:
        print("Extraction des résultats par commune...")
        commune_results = extract_commune_results(bfs_data)
        
        print("Export CSV communes...")
        write_csv(commune_results, OUTPUT_COMMUNES_FILE)
        
        print("\n=== Extraction des noms de votations ===")
        ballot_names = extract_ballot_names(bfs_data)
        
        print("\n=== Traitement des profils ===")
        process_votation_results()
        
        print("\nTerminé !")
    else:
        print("Échec de la récupération des données")