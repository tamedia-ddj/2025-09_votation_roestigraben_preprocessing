#!/usr/bin/env python3
"""
Script complet d'harmonisation des communes suisses traversées par les Intercity 1 et 21
et création du fichier final des profils avec les résultats de votation.
"""

import pandas as pd
import requests
from datetime import datetime
import re
import sys
from pathlib import Path

# =============================================================================
# CONFIGURATION - ETAPES
# 1. ENTRER "BFS_URL" JSON DU JOUR DE VOTATION
# 2. ENTRER "VOTING_DATE" JOUR DE VOTATION (YYYY-MM-DD)
# 3. LANCER LE SCRIPT
# =============================================================================

# Données de base des votations
# Aller chercher l'URL du JSON de la votation sur:
# https://opendata.swiss/de/dataset/echtzeitdaten-am-abstimmungstag-zu-eidgenoessischen-abstimmungsvorlagen

# -----------------------------------------------------------------------------
BFS_URL = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32006477/master"
BFS_URL = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32006477/master" # 9 juin 2024
BFS_URL = "https://ogd-static.voteinfo-app.ch/v1/ogd/sd-t-17-02-20241124-eidgAbstimmung.json" # 24 novembre 2024
VOTING_DATE = "2024-11-24"  # YYYY-MM-DD
# -----------------------------------------------------------------------------

TODAY = datetime.now().strftime("%d-%m-%Y")

# Fichiers InterCity d'entrée (pour l'harmonisation)
IC_INPUT_FILES = {
    "ic_1": "input/processed/InterCity_1_communes.csv",
    "ic_21": "input/processed/InterCity_21_communes.csv"
}

# Fichiers de référence
CANTON_FILE = "input/processed/canton_iso2.csv"
TRANSLATION_FILE = "input/processed/translations.csv"

# Fichiers de sortie
HARMONIZED_FILE = f"output/intercity_harmonized_{VOTING_DATE}.csv"
FINAL_OUTPUT_FILE = f"output/profil_results_{VOTING_DATE}.csv"
BALLOTS_INFO_FILE = f"output/ballot_name_{VOTING_DATE}.csv"

# Période pour les mutations communales
MUTATION_START_DATE = "01-01-2024"
MUTATION_END_DATE = TODAY

# Date de référence pour les niveaux géographiques
GEOLEVEL_DATE = TODAY

# =============================================================================


def load_intercity_data():
    """
    Charge et unifie les données InterCity 1 et 21
    """
    print("📊 Chargement des données InterCity...")
    
    try:
        # Charger IC1
        ic1_df = pd.read_csv(IC_INPUT_FILES["ic_1"])
        ic1_df = ic1_df[["order_ic1", "GMDNR"]].copy()
        ic1_df["ligne"] = "ic1"
        ic1_df.rename(columns={"order_ic1": "order_ic"}, inplace=True)
        
        # Charger IC21
        ic21_df = pd.read_csv(IC_INPUT_FILES["ic_21"])
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
    Charge la table de traduction locale
    """
    print("🌐 Chargement de la table de traduction...")
    
    try:
        translation_df = pd.read_csv(TRANSLATION_FILE)
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


def harmonize_communes():
    """
    Pipeline d'harmonisation des communes suisses
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
    result_df['GMDNR'] = result_df['FinalCode'].fillna(result_df['GMDNR'])
    
    # Convertir en integers
    result_df['GMDNR'] = result_df['GMDNR'].astype(int)
    
    # Garder seulement les colonnes nécessaires
    result_df = result_df[['order_ic', 'ligne', 'GMDNR']].copy()
    
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
                               left_on='GMDNR', 
                               right_on='CODE_OFS', 
                               how='left')
    
    # 7. Supprimer les doublons (garder celui avec le order_ic le plus bas)
    print("🧹 Suppression des doublons...")
    result_df = result_df.sort_values('order_ic').drop_duplicates(['GMDNR', 'ligne'], keep='first')
    
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
    final_columns = ['order_ic', 'ligne', 'GMDNR', 'Name_fr', 'iso2', 'fr', 'de']
    available_columns = [col for col in final_columns if col in result_df.columns]
    result_df = result_df[available_columns].copy()
    
    # Trier par ligne puis par order_ic
    result_df = result_df.sort_values(['ligne', 'order_ic'])
    
    # 11. Sauvegarder le résultat
    print(f"💾 Sauvegarde du fichier harmonisé: {HARMONIZED_FILE}")
    result_df.to_csv(HARMONIZED_FILE, index=False)
    
    print("\n" + "=" * 50)
    print(f"✅ Harmonisation terminée!")
    print(f"📊 {len(result_df)} communes traitées")
    print(f"📁 Fichier sauvé: {HARMONIZED_FILE}")
    
    # Afficher un aperçu
    print("\n📋 Aperçu des résultats:")
    print(result_df.head())
    
    return result_df


def fetch_bfs_results():
    """
    Récupère les résultats de votation depuis l'API BFS
    """
    print(f"📡 Récupération des données BFS depuis: {BFS_URL}")
    
    try:
        response = requests.get(BFS_URL)
        response.raise_for_status()
        data = response.json()
        
        results = []
        
        for ballot in data['schweiz']['vorlagen']:
            ballot_name = next(
                title['text'] 
                for title in ballot['vorlagenTitel'] 
                if title['langKey'] == 'fr'
            )
            
            for canton in ballot['kantone']:
                for commune in canton['gemeinden']:
                    results.append({
                        'id': commune['geoLevelnummer'],
                        'ballot_id': ballot['vorlagenId'],
                        'ballot_name': ballot_name,
                        'yes_pct': commune['resultat']['jaStimmenInProzent']
                    })
        
        df = pd.DataFrame(results)
        
        # Conversion de l'id en int pour la jointure
        df['id'] = df['id'].astype(int)
        
        ballot_ids = df['ballot_id'].unique()
        print(f"✅ {len(df)} résultats récupérés pour {len(ballot_ids)} objets de votation")
        print(f"   Ballot IDs: {', '.join(map(str, sorted(ballot_ids)))}")
        return df
        
    except requests.RequestException as e:
        print(f"❌ Erreur lors de la récupération: {e}")
        return None
    except KeyError as e:
        print(f"❌ Structure JSON inattendue: {e}")
        return None


def extract_ballot_titles():
    """
    Extrait les titres des ballots en format long avec une ligne par langue (FR/DE)
    """
    print("\n" + "📋 Extraction des titres des ballots")
    print("=" * 50)
    
    try:
        print(f"📡 Récupération des données depuis: {BFS_URL}")
        response = requests.get(BFS_URL)
        response.raise_for_status()
        data = response.json()
        
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
        
        # Sauvegarder le fichier
        df_ballots.to_csv(BALLOTS_INFO_FILE, index=False)
        
        print(f"✅ {len(df_ballots)} entrées créées ({len(df_ballots)//2} ballots × 2 langues)")
        print(f"📁 Fichier sauvé: {BALLOTS_INFO_FILE}")
        
        # Afficher les ballots avec leurs IDs
        ballot_ids = df_ballots['ballot-id'].unique()
        print(f"📊 Ballot IDs: {', '.join(map(str, sorted(ballot_ids)))}")
        
        print("\n📋 Aperçu des titres:")
        for ballot_id in sorted(ballot_ids):
            print(f"  Ballot {ballot_id}:")
            ballot_data = df_ballots[df_ballots['ballot-id'] == ballot_id]
            for _, row in ballot_data.iterrows():
                print(f"    {row['langue']}: {row['title_long'][:60]}...")
            print()
        
        print("⚠️  Note: Les titres courts (title_short) doivent être ajoutés manuellement dans le fichier CSV")
        
        return df_ballots
        
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction des titres: {e}")
        return None


def create_final_results():
    """
    Crée le fichier final avec les résultats de votation
    """
    print("\n" + "🗳️ Création du fichier final avec résultats de votation")
    print("=" * 50)
    
    # 1. Charger le fichier harmonisé
    try:
        print(f"📂 Chargement du fichier harmonisé: {HARMONIZED_FILE}")
        harmonized_df = pd.read_csv(HARMONIZED_FILE)
        print(f"✅ {len(harmonized_df)} communes harmonisées chargées")
    except Exception as e:
        print(f"❌ Erreur lors du chargement du fichier harmonisé: {e}")
        return False
    
    # 2. Récupérer les résultats BFS
    bfs_results = fetch_bfs_results()
    if bfs_results is None:
        print("❌ Impossible de récupérer les résultats BFS")
        return False
    
    # 3. Jointure pour tous les ballot_id
    print("🔗 Fusion des données harmonisées avec tous les résultats...")
    merged = harmonized_df.merge(
        bfs_results, 
        left_on='GMDNR', 
        right_on='id', 
        how='inner'  # Inner join pour garder seulement les communes avec résultats
    )
    
    # 4. Vérification des communes manquantes
    communes_with_results = set(bfs_results['id'].unique())
    communes_harmonized = set(harmonized_df['GMDNR'].unique())
    missing_communes = communes_harmonized - communes_with_results
    
    if missing_communes:
        print(f"⚠️ {len(missing_communes)} communes harmonisées sans résultats de votation")
        missing_df = harmonized_df[harmonized_df['GMDNR'].isin(missing_communes)]
        for _, row in missing_df.iterrows():
            print(f"  - {row['Name_fr']} - ID: {row['GMDNR']} - Ligne: {row['ligne']}")
    
    # 5. Renommer les colonnes selon le format attendu
    print("🔄 Renommage des colonnes...")
    column_mapping = {
        'order_ic': 'order',
        'ligne': 'ligne',  # garder tel quel mais changer ic1/ic21 -> ic_1/ic_21
        'Name_fr': 'GMDNAME',
        'iso2': 'KTN_abr', 
        'fr': 'GMDNAME_FR',
        'de': 'GMDNAME_DE'
    }
    
    # Appliquer le renommage
    final_df = merged.rename(columns=column_mapping)
    
    # Corriger les valeurs de ligne pour correspondre au format attendu
    final_df['ligne'] = final_df['ligne'].map({'ic1': 'ic_1', 'ic21': 'ic_21'})
    
    # 6. Sélectionner et ordonner les colonnes finales
    final_columns = ['GMDNR', 'order', 'ligne', 'GMDNAME', 'GMDNAME_FR', 'GMDNAME_DE', 'KTN_abr', 'ballot_id', 'yes_pct']
    final_df = final_df[final_columns].copy()
    
    # 7. Tri final: ballot_id, ligne, order
    final_df = final_df.sort_values(['ballot_id', 'ligne', 'order'])
    
    # 8. Export
    try:
        ballot_ids = final_df['ballot_id'].unique()
        final_df.to_csv(FINAL_OUTPUT_FILE, index=False)
        print(f"✅ Fichier final exporté: {FINAL_OUTPUT_FILE}")
        print(f"  - {len(final_df)} lignes")
        print(f"  - {len(ballot_ids)} objets de votation: {', '.join(map(str, sorted(ballot_ids)))}")
        print(f"  - {len(final_df['ligne'].unique())} lignes InterCity")
        
        # Aperçu des colonnes finales
        print(f"\n📋 Colonnes du fichier final: {list(final_df.columns)}")
        print(f"\n📊 Aperçu des données:")
        print(final_df.head())
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'export: {e}")
        return False


def main():
    """
    Pipeline complet: harmonisation + résultats finaux
    """
    print(f"🎯 === Pipeline complet de traitement des votations {VOTING_DATE} ===\n")
    
    try:
        # Étape 1: Harmonisation des communes
        harmonized_data = harmonize_communes()
        
        # Étape 2: Extraction des titres des ballots
        ballots_info = extract_ballot_titles()
        
        # Étape 3: Création du fichier final avec résultats
        success = create_final_results()
        
        if success:
            print(f"\n🎉 Pipeline terminé avec succès !")
            print(f"📁 Fichiers créés:")
            print(f"  - Harmonisé: {HARMONIZED_FILE}")
            print(f"  - Infos ballots: {BALLOTS_INFO_FILE}")
            print(f"  - Final: {FINAL_OUTPUT_FILE}")
        else:
            print(f"\n❌ Erreurs lors du pipeline")
            
    except KeyboardInterrupt:
        print("\n⏹️ Script interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()