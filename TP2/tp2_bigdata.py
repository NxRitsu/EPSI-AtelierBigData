from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, explode, split, regexp_extract
import os
import shutil
import missingno as ms
import pandas
import matplotlib.pyplot as pl

# Création d'une session Spark
spark = SparkSession.builder \
    .appName("Nettoyage données Netflix") \
    .getOrCreate()

# Réduire les logs pour éviter trop d'avertissements
spark.sparkContext.setLogLevel("ERROR")

# Chargement des données
file_path = "netflix1.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Remplacement des valeurs "Not Given", "N/A" et "" par "null"
df_cleaned = df.replace(["Not Given", "N/A", ""], [None, None, None])

# Vérification si les valeurs ont bien été modifiées
df_cleaned.show()

# Conversion en DataFrame Pandas (en prenant les 25 premières lignes)
pandas_df = df_cleaned.limit(25).toPandas()

# Visualisation avec Missingno avant nettoyage
ms.bar(pandas_df)
pl.title("Visualisation des valeurs manquantes avant nettoyage")
pl.show() # Il y a des données manquantes dans les colonnes "director" et "country"

# Mise en place d'un filtre pour supprimer les lignes manquantes (country et director)
df_cleaned = df_cleaned.dropna(subset=["director", "country"])

# On nettoie la colonne "release_year" car elle a une donnée "40 min" dedans, on veut donc uniquement 4 chiffres
df_cleaned = df_cleaned.withColumn("release_year", regexp_extract(col("release_year"), r'(\d{4})', 0))

# Conversion à nouveau en Pandas pour visualiser après nettoyage
pandas_cleaned_df = df_cleaned.limit(25).toPandas()

# Visualiser les données manquantes après nettoyage
ms.bar(pandas_cleaned_df)
pl.title("Répartition des valeurs manquantes après nettoyage")
pl.show() # Le filtrage a bien amélioré la qalité globale

# Division des genres multiples en genres individuels en utilisant la virgule
df_cleaned = df_cleaned.withColumn("listed_in", explode(split(col("listed_in"), ", ")))

# Compter les occurrences de chaque genre
genre_counts = df_cleaned.groupBy("listed_in").agg(count("listed_in").alias("count")).orderBy(col("count").desc())

# Visualisation des genres les plus fréquents
genre_counts_pd = genre_counts.limit(10).toPandas()
pl.figure(figsize=(10, 6))
pl.bar(genre_counts_pd["listed_in"], genre_counts_pd["count"], color='skyblue')
pl.title("Top 10 des genres les plus fréquents")
pl.xlabel("Genres")
pl.ylabel("Nombre d'occurrences")
pl.xticks(rotation=45, ha="right")
pl.tight_layout()
pl.show()

# Compter le nombre de films par année
year_counts = df_cleaned.groupBy("release_year").agg(count("release_year").alias("count")).orderBy(col("release_year"))

# Visualisation des films par année
year_counts_pd = year_counts.toPandas()
pl.figure(figsize=(12, 6))
pl.plot(year_counts_pd["release_year"], year_counts_pd["count"], marker="o", linestyle="-", color="b")
pl.title("Nombre de films par année")
pl.xlabel("Année")
pl.ylabel("Nombre de films")
pl.grid(True)
pl.xticks(rotation=90)
pl.tight_layout()
pl.show()

# Sauvegarde des données nettoyées dans un nouveau fichier CSV dispo dans le dossirer "cleaned_netflix_data" (je n'arrive pas a modifier le nom final du fichier...)
output_file = "cleaned_netflix_data"
df_cleaned.write.csv(output_file, header=True, mode="overwrite")