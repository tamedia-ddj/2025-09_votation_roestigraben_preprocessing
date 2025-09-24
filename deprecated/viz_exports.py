import pandas as pd
import requests
import re
from datetime import datetime

# Configuration
# Les liens vers les JSON sont sur cette page: https://opendata.swiss/de/dataset/echtzeitdaten-am-abstimmungstag-zu-eidgenoessischen-abstimmungsvorlagen
BFS_URL = "https://ogd-static.voteinfo-app.ch/v1/ogd/sd-t-17-02-20250209-eidgAbstimmung.json"
VOTING_DATE = "2025-02-09"  # YYYY/MM/DD Juste pour nommer le fichier de sortie
IC_FILES = {
    "ic_1": "input/processed/InterCity_1_communes.csv",
    "ic_21": "input/processed/InterCity_21_communes.csv"
}
OUTPUT_FILE = f"output/profil_results_{VOTING_DATE}.csv"

def fetch_bfs_results():
    """
    Récupère les résultats de votation depuis l'API BFS
    """
    print(f"Récupération des données BFS depuis: {BFS_URL}")
    
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
        
        print(f"✓ {len(df)} résultats récupérés pour {len(df['ballot_id'].unique())} objets de votation")
        return df
        
    except requests.RequestException as e:
        print(f"✗ Erreur lors de la récupération: {e}")
        return None
    except KeyError as e:
        print(f"✗ Structure JSON inattendue: {e}")
        return None

def load_intercity_lines(files_dict):
    """
    Charge et standardise les données des lignes InterCity
    """
    combined_data = []
    
    # Mapping des noms de colonnes order
    order_columns = {
        'ic_1': 'order_ic1',
        'ic_21': 'order_ic21'
    }
    
    for line_name, file_path in files_dict.items():
        try:
            df = pd.read_csv(file_path)
            
            # Standardisation des colonnes
            order_col = order_columns.get(line_name, f'order_{line_name}')
            df_clean = df.rename(columns={
                order_col: 'order'
            }).assign(ligne=line_name)
            
            # Sélection des colonnes nécessaires (avec GMDNAME_DE)
            df_clean = df_clean[['order', 'ligne', 'GMDNAME', 'GMDNAME_FR', 'GMDNAME_DE', 'GMDNR', 'KTN_abr', 'KTNR']]
            
            combined_data.append(df_clean)
            print(f"✓ {line_name}: {len(df_clean)} communes chargées")
            
        except FileNotFoundError:
            print(f"✗ Fichier non trouvé: {file_path}")
        except Exception as e:
            print(f"✗ Erreur lors du chargement de {line_name}: {e}")
    
    if combined_data:
        result = pd.concat(combined_data, ignore_index=True)
        print(f"✓ Total: {len(result)} communes sur {len(result['ligne'].unique())} lignes")
        return result
    else:
        return None

def merge_and_export(bfs_data, ic_data, output_file):
    """
    Joint les données et exporte le CSV final
    """
    if bfs_data is None or ic_data is None:
        print("✗ Données manquantes pour la fusion")
        return False
    
    # Jointure
    merged = ic_data.merge(
        bfs_data, 
        left_on='GMDNR', 
        right_on='id', 
        how='left'
    )
    
    # Vérification et gestion des communes sans résultats
    missing_results = merged[merged['ballot_id'].isna()]
    if not missing_results.empty:
        print(f"⚠ Communes sans résultats de votation (probablement fusionnées) :")
        for _, row in missing_results.iterrows():
            print(f"  - {row['GMDNAME']} ({row['GMDNAME_FR']}) - ID: {row['GMDNR']} - Ligne: {row['ligne']}")
        
        print(f"\n→ Suppression de {len(missing_results)} communes sans résultats")
        # Supprimer les communes sans résultats
        merged = merged[merged['ballot_id'].notna()]
    
    # Tri et sélection finale
    final_df = merged.sort_values(['ballot_id', 'ligne', 'order'])
    final_df = final_df.drop(columns=['ballot_name'], errors='ignore')
    
    # Export
    try:
        final_df.to_csv(output_file, index=False)
        print(f"✓ Fichier exporté: {output_file}")
        print(f"  - {len(final_df)} lignes")
        print(f"  - {len(final_df['ballot_id'].unique())} objets de votation")
        return True
    except Exception as e:
        print(f"✗ Erreur lors de l'export: {e}")
        return False

def main():
    """
    Pipeline principal
    """
    print(f"=== Traitement des résultats de votation {VOTING_DATE} ===\n")
    
    # 1. Récupération des résultats BFS
    bfs_results = fetch_bfs_results()
    
    # 2. Chargement des lignes InterCity
    print()
    ic_data = load_intercity_lines(IC_FILES)
    
    # 3. Fusion et export
    print()
    success = merge_and_export(bfs_results, ic_data, OUTPUT_FILE)
    
    if success:
        print(f"\n✅ Traitement terminé avec succès !")
    else:
        print(f"\n❌ Erreurs lors du traitement")

if __name__ == "__main__":
    main()


print(missing_results)

