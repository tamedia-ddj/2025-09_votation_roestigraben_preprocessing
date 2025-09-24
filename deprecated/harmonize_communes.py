#!/usr/bin/env python3
"""
Script d'harmonisation des communes suisses avec gestion des fusions communales
et récupération des données géographiques via les APIs BFS.
"""

import pandas as pd
import requests
from datetime import datetime
import re
import warnings
import sys
from pathlib import Path

# =============================================================================
# CONFIGURATION - Modifiez ces paramètres selon vos besoins
# =============================================================================

# Données de base
BFS_URL = "https://ogd-static.voteinfo-app.ch/v1/ogd/sd-t-17-02-20250209-eidgAbstimmung.json"
VOTING_DATE = "2025-02-09"  # YYYY-MM-DD
TODAY = datetime.now().strftime("%d-%m-%Y")

# Fichiers InterCity
IC_FILES = {
    "ic_1": "input/processed/InterCity_1_communes.csv",
    "ic_21": "input/processed/InterCity_21_communes.csv"
}

# Fichiers de sortie
OUTPUT_FILE = f"output/profil_results_{VOTING_DATE}.csv"

# Table de correspondance cantons
CANTON_FILE = "input/processed/canton_iso2.csv"

# Période pour les mutations communales
MUTATION_START_DATE = "01-01-2024"
MUTATION_END_DATE = TODAY

# Configuration Google Sheets pour les traductions
GOOGLE_SHEET_ID = "13OS24WhvYQj220KY06QKd3pAzkEtdfEyUmxemAt9y5s"
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid=0"

# Date de référence pour les niveaux géographiques
GEOLEVEL_DATE = "18-10-2020"

# =============================================================================


def load_intercity_data():
    """
    Charge et unifie les données InterCity 1 et 21
    """
    print("📊 Chargement des données InterCity...")
    
    try:
        # Charger IC1
        ic1_df = pd.read_csv(IC_FILES["ic_1"])
        ic1_df = ic1_df[["order_ic1", "GMDNR"]].copy()
        ic1_df["ligne"] = "ic1"
        ic1_df.rename(columns={"order_ic1": "order_ic"}, inplace=True)
        
        # Charger IC21
        ic21_df = pd.read_csv(IC_FILES["ic_21"])
        ic21_df = ic21_df[["order_ic21", "GMDNR"]].copy()
        ic21_df["ligne"] = "ic21"
        ic21_df.rename(columns={"order_ic21": "order_ic"}, inplace=True)
        
        # Fusionner les DataFrames
        unified_df = pd.concat([ic1_df, ic21_df], ignore_index=True)
        
        print(f"✅ Données chargées: {len(ic1_df)} communes IC1, {len(ic21_df)} communes IC21")
        return unified_df
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement des fichiers InterCity: {e}")
        sys.exit(1)


def get_communes_mutations_bfs(date1, date2):
    """
    Récupère les mutations communales via l'API BFS
    """
    print(f"🔄 Récupération des mutations communales ({date1} -> {date2})...")
    
    url = (f"https://www.agvchapp.bfs.admin.ch/api/communes/mutations?"
           f"includeTerritoryExchange=false&Deleted=True&Created=True"
           f"&startPeriod={date1}&endPeriod={date2}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse CSV response
        from io import StringIO
        mutations_df = pd.read_csv(StringIO(response.text))
        
        print(f"✅ {len(mutations_df)} mutations récupérées")
        return mutations_df
        
    except Exception as e:
        print(f"⚠️ Avertissement: Erreur lors de la récupération des mutations: {e}")
        print("Continuant sans données de mutation...")
        return pd.DataFrame(columns=['InitialCode', 'TerminalCode', 'MutationDate'])


def create_mutation_key(mutations_df):
    """
    Crée la clé de correspondance des mutations
    """
    if mutations_df.empty:
        return pd.DataFrame(columns=['InitialCode', 'FinalCode'])
    
    print("🔑 Création de la clé de mutations...")
    
    # Trier par date et garder la dernière mutation pour chaque commune
    # Le format des dates peut être DD.MM.YYYY, donc on utilise format='mixed' pour la flexibilité
    mutations_df['MutationDate'] = pd.to_datetime(mutations_df['MutationDate'], format='mixed', dayfirst=True)
    mutation_key = (mutations_df
                   .sort_values('MutationDate')
                   .groupby('InitialCode')
                   .agg({'TerminalCode': 'last'})
                   .reset_index()
                   .rename(columns={'TerminalCode': 'FinalCode'}))
    
    print(f"✅ {len(mutation_key)} correspondances créées")
    return mutation_key


def get_geolevel(date_str):
    """
    Récupère les niveaux géographiques via l'API BFS
    """
    print(f"🌍 Récupération des niveaux géographiques pour {date_str}...")
    
    url = (f"https://sms.bfs.admin.ch/WcfBFSSpecificService.svc/AnonymousRest/"
           f"communes/levels?startPeriod={date_str}&endPeriod={date_str}"
           f"&useBfsCode=false&labelLanguages=fr&format=csv")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        from io import StringIO
        spatial_df = pd.read_csv(StringIO(response.text))
        
        # Sélectionner les colonnes nécessaires
        spatial_df = spatial_df[['CODE_OFS', 'Name_fr', 'HR_HGDE_HIST_L1']].copy()
        
        print(f"✅ {len(spatial_df)} communes récupérées")
        return spatial_df
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des niveaux géographiques: {e}")
        sys.exit(1)


def load_canton_iso2():
    """
    Charge la table de correspondance des cantons ISO2
    """
    print("🏞️ Chargement de la table canton_iso2...")
    
    try:
        canton_df = pd.read_csv(CANTON_FILE)
        print(f"✅ {len(canton_df)} cantons chargés")
        return canton_df
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement de {CANTON_FILE}: {e}")
        sys.exit(1)


def load_translation_table():
    """
    Charge la table de traduction depuis Google Sheets
    """
    print("🌐 Chargement de la table de traduction...")
    
    try:
        translation_df = pd.read_csv(GOOGLE_SHEET_URL)
        print(f"✅ {len(translation_df)} traductions chargées")
        return translation_df
        
    except Exception as e:
        print(f"⚠️ Avertissement: Erreur lors du chargement des traductions: {e}")
        print("Continuant sans table de traduction...")
        return pd.DataFrame(columns=['polg_name', 'fr', 'de'])


def remove_canton_abbreviations(text):
    """
    Supprime les abréviations cantonales à la fin des noms (pattern: ' (XX)')
    """
    if pd.isna(text):
        return text
    return re.sub(r' \([A-Z]{2}\)$', '', str(text))


def main():
    """
    Pipeline principal d'harmonisation
    """
    print("🚀 Début de l'harmonisation des communes suisses")
    print("=" * 50)
    
    # Créer le dossier de sortie s'il n'existe pas
    Path("output").mkdir(exist_ok=True)
    
    # 1. Charger les données InterCity
    intercity_df = load_intercity_data()
    
    # 2. Récupérer les mutations communales
    mutations_df = get_communes_mutations_bfs(MUTATION_START_DATE, MUTATION_END_DATE)
    mutation_key = create_mutation_key(mutations_df)
    
    # 3. Fusionner avec les mutations
    print("🔗 Fusion avec les données de mutations...")
    result_df = intercity_df.merge(mutation_key, 
                                  left_on='GMDNR', 
                                  right_on='InitialCode', 
                                  how='left')
    
    # Utiliser GMDNR quand FinalCode est vide
    result_df['FinalCode'] = result_df['FinalCode'].fillna(result_df['GMDNR'])
    
    # Garder seulement les colonnes nécessaires
    result_df = result_df[['order_ic', 'ligne', 'FinalCode']].copy()
    
    # 4. Récupérer les niveaux géographiques
    spatial_df = get_geolevel(GEOLEVEL_DATE)
    
    # 5. Charger et fusionner avec canton_iso2
    canton_df = load_canton_iso2()
    spatial_df = spatial_df.merge(canton_df, 
                                 left_on='HR_HGDE_HIST_L1', 
                                 right_on='order', 
                                 how='left')
    spatial_df = spatial_df[['CODE_OFS', 'Name_fr', 'iso2']].copy()
    
    # 6. Fusionner avec les données géographiques
    print("🗺️ Fusion avec les données géographiques...")
    result_df = result_df.merge(spatial_df, 
                               left_on='FinalCode', 
                               right_on='CODE_OFS', 
                               how='left')
    
    # 7. Supprimer les doublons (garder celui avec le order_ic le plus bas)
    print("🧹 Suppression des doublons...")
    result_df = result_df.sort_values('order_ic').drop_duplicates(['FinalCode', 'ligne'], keep='first')
    
    # 8. Charger et fusionner avec la table de traduction
    translation_df = load_translation_table()
    
    if not translation_df.empty:
        print("🔤 Application des traductions...")
        result_df = result_df.merge(translation_df[['polg_name', 'fr', 'de']], 
                                   left_on='Name_fr', 
                                   right_on='polg_name', 
                                   how='left')
        
        # Utiliser Name_fr comme fallback pour les traductions manquantes
        result_df['fr'] = result_df['fr'].fillna(result_df['Name_fr'])
        result_df['de'] = result_df['de'].fillna(result_df['Name_fr'])
    else:
        # Pas de traductions disponibles, utiliser Name_fr pour les deux langues
        result_df['fr'] = result_df['Name_fr']
        result_df['de'] = result_df['Name_fr']
    
    # 9. Supprimer les abréviations cantonales
    print("✂️ Suppression des abréviations cantonales...")
    result_df['fr'] = result_df['fr'].apply(remove_canton_abbreviations)
    result_df['de'] = result_df['de'].apply(remove_canton_abbreviations)
    
    # 10. Nettoyer et finaliser le DataFrame
    final_columns = ['order_ic', 'ligne', 'FinalCode', 'Name_fr', 'iso2', 'fr', 'de']
    available_columns = [col for col in final_columns if col in result_df.columns]
    result_df = result_df[available_columns].copy()
    
    # 11. Sauvegarder le résultat
    print(f"💾 Sauvegarde du fichier: {OUTPUT_FILE}")
    result_df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "=" * 50)
    print(f"✅ Harmonisation terminée!")
    print(f"📊 {len(result_df)} communes traitées")
    print(f"📁 Fichier sauvé: {OUTPUT_FILE}")
    
    # Afficher un aperçu
    print("\n📋 Aperçu des résultats:")
    print(result_df.head())
    
    return result_df


if __name__ == "__main__":
    try:
        result = main()
    except KeyboardInterrupt:
        print("\n⏹️ Script interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        sys.exit(1)