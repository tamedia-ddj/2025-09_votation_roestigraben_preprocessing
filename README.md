# Swiss Vote Pipeline

Script "roestigraben_pipeline.py" for harmonizing Swiss municipalities and extracting federal voting results to analyze geographic patterns.

## Purpose

Analyzes voting results along InterCity train lines to visualize regional differences, as illustrated in this article:
https://www.24heures.ch/si-le-roestigraben-etait-une-ligne-de-train-407691020754

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
│   └── translations.csv            # Municipality translations
└── output/
    ├── intercity_harmonized_{date}.csv  # Harmonized municipalities
    ├── ballot_name_{date}.csv           # Ballot titles
    └── profil_results_{date}.csv        # Final results
```

## Usage

1. Update the configuration at the top of the script:

   - `BFS_URL`: JSON URL from the voting data page
   - `VOTING_DATE`: Date in YYYY-MM-DD format

2. Run the pipeline:

```bash
python vote_pipeline.py
```

3. Complete the short titles in `ballot_name_{date}.csv`:
   - The script generates long titles automatically
   - Edit the `title_short` column manually with concise ballot names

The script automatically generates three output files with municipal merger harmonization and complete results extraction.
