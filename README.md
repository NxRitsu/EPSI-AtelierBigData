# Nettoyage et analyse des données
Ce projet utilise PySpark pour analyser des jeux de données.

## Technologies

- Python, PySpark, Pandas
- Matplotlib, Missingno
- Google Collab

## Installation

1. Clonez le dépôt :
   ```bash
   git clone https://github.com/EPSI-AtelierBigData.git
   cd netflix-data-cleaning
   ```
2. Installez les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

# TP 1

Ce TP a été fait sur Google collab, il y a donc juste a importer le fichier TP1.ipynb dans Google Collab.

## Fonctionnalités

### Nettoyage des Données
- Suppression des lignes avec des valeurs nulles dans les colonnes clés.
- Conversion des dates au format `yyyy-MM-dd`.

### Analyses et Transformations
- Filtrage des films selon des critères spécifiques.
- Moyennes des notes des films par studio et par réalisateur.
- Extraction et analyse des genres multiples.

# TP 2

## Fonctionnalités

- Gestion des valeurs manquantes (`"Not Given"`, `"N/A"`).
- Extraction et normalisation des années de sortie.
- Visualisation des genres les plus fréquents et des tendances par année.
- Exportation des données nettoyées au format CSV.

## Utilisation TP 2

1. Placez le fichier `netflix1.csv` dans le répertoire.
2. Modifiez `file_path` pour pointer vers le fichier.
3. Exécutez le script :
   ```bash
   python tp2_bigdata.py
   ```
4. Les données nettoyées sont sauvegardées dans le dossier `cleaned_netflix_data`.