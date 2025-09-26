# Swiss Vote Pipeline

Script "roestigraben_pipeline.py" for harmonizing Swiss municipalities and extracting federal voting results to analyze geographic patterns.

## Purpose

Analyzes voting results along InterCity train lines to visualize regional differences, as illustrated in this article:
https://www.24heures.ch/si-le-roestigraben-etait-une-ligne-de-train-407691020754

## Scripts

### Main Pipeline (`roestigraben_pipeline.py`)
Full harmonization pipeline with municipality merger management via BFS APIs.

### Backup Script (`backup_votation_pipeline.py`)
Simplified backup script without municipality harmonization. Uses pre-harmonized municipality files and directly extracts voting results. Useful when API access is limited or for quick processing with existing harmonized data.

## Data Sources

- **BFS Municipality API**: Municipal geocode harmonization and merger management
- **FSO API**: Federal voting results extraction from JSON format
- **Reference data**: Official voting data page: https://opendata.swiss/de/dataset/echtzeitdaten-am-abstimmungstag-zu-eidgenoessischen-abstimmungsvorlagen

## File Structure

```
├── input/processed/
│   ├── InterCity_1_communes.csv    # IC1 line municipalities
│   ├── InterCity_21_communes.csv   # IC21 line municipalities
│   ├── canton_iso2.csv             # Canton codes
│   ├── translations.csv            # Municipality translations
│   └── intercity_harmonized_{date}.csv  # Pre-harmonized data (for backup)
└── output/
    ├── intercity_harmonized_{date}.csv     # Harmonized municipalities
    ├── ballot_name_{date}.csv              # Ballot titles
    ├── profil_results_{date}.csv           # Final results
    ├── backup_profil_results_{date}.csv    # Backup results
    └── backup_ballot_names_{date}.csv      # Backup ballot names
```

## Usage

### Main Pipeline
1. Update the configuration at the top of the script:
   - `BFS_URL`: JSON URL from the voting data page
   - `VOTING_DATE`: Date in YYYY-MM-DD format

2. Run the pipeline:
```bash
python roestigraben_pipeline.py
```

### Backup Script
1. Update configuration variables:
   - `BFS_URL`: JSON URL from the voting data page
   - `VOTING_DATE`: Date in YYYY-MM-DD format
   - `INTERCITY_FILE`: Path to pre-harmonized municipality file

2. Run the backup script:
```bash
python backup_votation_pipeline.py
```

3. Complete the short titles in `ballot_name_{date}.csv` or `backup_ballot_names_{date}.csv`:
   - The scripts generate long titles automatically
   - Edit the `title_short` column manually with concise ballot names

The main script automatically generates three output files with municipal merger harmonization and complete results extraction. The backup script provides a faster alternative using existing harmonized data.