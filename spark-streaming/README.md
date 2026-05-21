# TP — Introduction à Spark Structured Streaming

## Objectifs pédagogiques

À la fin de ce TP, vous serez capable de :

* Déployer un cluster Spark avec Docker.
* Comprendre le principe du micro-batch dans Spark Structured Streaming.
* Lire des données en flux continu depuis un répertoire.
* Observer le comportement du streaming lors de l’ajout et de la suppression de fichiers.
* Utiliser les DataFrames Spark pour effectuer des agrégations en temps réel.

---

# 1. Architecture du TP

Le cluster utilisé dans ce TP repose sur :

* 1 Spark Master
* 3 Spark Workers
* 1 serveur Jupyter avec PySpark

Le déploiement est réalisé avec le fichier `docker-compose.yml` fourni.

---

# 2. Lancement de l’environnement

## 2.1 Démarrer les conteneurs

Depuis le répertoire contenant le `docker-compose.yml` :

```bash
docker compose up -d
```

---

## 2.2 Vérifier les services

### Interface Spark Master

Ouvrir :

```text
http://localhost:8080
```

Vous devez voir :

* le master Spark ;
* les 3 workers connectés.

---

### Interface JupyterLab

Ouvrir :

```text
http://localhost:8888
```

Token :

```text
spark
```

---

# 3. Préparation du dossier de streaming 

> Attention ! La prépartion est seulement dans le cas ou le dossier n'exite pas !

Dans le conteneur Jupyter, créer un dossier :

```bash
mkdir -p /opt/spark/stream-read
```

Ce dossier sera surveillé par Spark Streaming.

Tester l'ajour la possibilité de mettre le dataset ``adult_new_data.csv``, Dans le cas ou ce n'est pas possible, faites appel à votre super prof ! 

Il y a des restrictions de permissions sur les dossiers ``work`` et ``stream-read`` qui sont montés depuis la machine hôte qu'il faut fixer :

- Sur Linux (dans le cas de codespace par exemple):

```bash
# Accorder les permissions pleines (attention à la sécurité, dans la vraie vie)
sudo chmod -R 777 work stream-read
```


- Sur Widows via powerShell:

```bash
# Accorder les permissions complètes
icacls "C:\chemin\vers\spark-streaming\work" /grant:r "%username%:(F)" /t /c
icacls "C:\chemin\vers\spark-streaming\stream-read" /grant:r "%username%:(F)" /t /c
```


---

# 4. Première approche du streaming

## 4.1 Création de la session Spark

Créer une cellule dans le notebook.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CensusStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark
```
**Question**: A quoi sert chaque argument ? 

---

## 4.2 Définition du schéma

Nous allons lire des fichiers CSV représentant des données de recensement.

```python
from pyspark.sql.types import *

schema_defined = StructType([
    StructField('age', LongType(), True),
    StructField('workclass', StringType(), True),
    StructField('fnlwgt', LongType(), True),
    StructField('education', StringType(), True),
    StructField('education-num', LongType(), True),
    StructField('marital-status', StringType(), True),
    StructField('occupation', StringType(), True),
    StructField('relationship', StringType(), True),
    StructField('race', StringType(), True),
    StructField('sex', StringType(), True),
    StructField('capital-gain', LongType(), True),
    StructField('capital-loss', LongType(), True),
    StructField('hours-per-week', LongType(), True),
    StructField('native-country', StringType(), True),
    StructField('income', StringType(), True)
])
```

**Question** : A quoi sert cette étape de creation de schema ?

---

# 5. Lecture en flux continu

## 5.1 Lecture du répertoire surveillé

```python
STREAM_PATH = "/opt/spark/stream-read"
stream_memory_query = (spark.readStream
                         .format("csv")
                         .schema(schema_defined) 
                         .option("header", "true")
                         .option("maxFilesPerTrigger", 1)
                         .load(STREAM_PATH)
                         .writeStream
                         .outputMode("append")
                         .format("memory")
                         .queryName("stream_data_check")
                         .trigger(processingTime="5 seconds")
                         .start())

print("Streaming query started, writing to memory table 'stream_data_check'.")
print("Waiting for data to be processed and appear in the table...")
```

**Question** : Il est possible d'avoir la requête ``stream_memory_query`` en deux parties, pouvez vous la refaire autrement ? (L'idée est de comprendre ce qu'elle fais exactement)

---

## 5.2 Affichage temps réel

```python
import time
from IPython.display import display

# Give the stream some time to process the initial files and the new file
print("Fetching data from 'stream_data' table every 5")
for i in range(30):
    if stream_memory_query.isActive:
        # Query the memory table to see what the stream has processed
        print(f"--- Snapshot {i+1} at {time.strftime('%H:%M:%S')} ---")
        display(spark.sql("SELECT count(*) FROM stream_data_check").show(truncate=False))
        time.sleep(5) # Wait for the next trigger
    else:
        print("Stream query became inactive.")
        break

print("Finished observing memory table.")

# Stop the query after observation
if stream_memory_query.isActive:
    stream_memory_query.stop()
    print("Streaming query 'stream_data_check' explicitly stopped.")
```

---

# 6. Expérimentation pédagogique

## 6.1 Ajouter des fichiers progressivement

Créer plusieurs petits fichiers CSV :

* `adult1.csv`
* `adult2.csv`
* `adult3.csv`

Ajouter les fichiers un par un dans le dossier stream-read (manuellement) ou par  :

```bash
cp part1.csv /opt/spark/stream-read/
```

Observer :

* le comportement de l'affichage micro-batch ;

---

## Questions

1. Pourquoi les données n’apparaissent-elles pas immédiatement ?
2. Quel est le rôle de `maxFilesPerTrigger` ?
3. Quelle différence existe-t-il entre batch et micro-batch ?

---

# 7. Suppression d’un fichier pendant le streaming

## Expérience

Pendant que le stream tourne :

1. Supprimer un fichier déjà traité.
2. Observer le comportement du flux.


---

## Questions

1. Les données disparaissent-elles du résultat ?
2. Pourquoi ?
3. Spark relit-il les anciens fichiers ?
4. Comment Spark mémorise-t-il les fichiers déjà traités ?
5. Que se passe t-il quand on remet le fichier supprimé ?

---

## Point pédagogique important

Spark Structured Streaming considère un fichier comme :

* traité une seule fois ;
* immuable ;
* définitivement intégré au flux.

La suppression du fichier source ne retire donc pas les données déjà ingérées.

---

# 8. Utilisation des tables mémoire

## 8.1 Création d’une table mémoire

```python
memory_query = (
    stream_df.writeStream
    .format("memory")
    .queryName("stream_table")
    .outputMode("append")
    .start()
)
```

---

## 8.2 Interroger les données en SQL

```python
spark.sql("SELECT * FROM stream_table LIMIT 10").show()
```

---

# 9. Agrégations temps réel avec DataFrames

## 9.1 Comptage des lignes

```python
from pyspark.sql.functions import count

count_df = stream_df.groupBy().agg(count("*").alias("total_rows"))
```

---

## 9.2 Affichage temps réel

```python
count_query = (
    count_df.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)
```

---

## Questions

1. Pourquoi utilise-t-on le mode `complete` ici ?
2. Quelle différence avec le mode `append` ?

---

# 10. GroupBy en streaming

## 10.1 Nombre de personnes par niveau d’éducation

```python
education_df = (
    stream_df
    .groupBy("education")
    .count()
)
```

---

## 10.2 Affichage du résultat

```python
education_query = (
    education_df.writeStream
    .format("console")
    .outputMode("complete")
    .start()
)
```

---

## Travail demandé

Créer d’autres agrégations :

* nombre de personnes par sexe ;
* nombre de personnes par pays ;
* moyenne des heures travaillées ;
* moyenne d’âge par profession.

---

# 11. Visualisation de l’évolution du flux

## Objectif

Créer une visualisation représentant :

* l’évolution du nombre total de lignes ;
* ou l’évolution d’une catégorie.

---

<!-- ## 11.1 Stockage des résultats dans une liste Python

```python
import time
import matplotlib.pyplot as plt

values = []

for i in range(10):
    result = spark.sql("SELECT COUNT(*) AS total FROM stream_table")
    total = result.collect()[0][0]
    values.append(total)
    print(f"Itération {i} : {total}")
    time.sleep(5)
```

---

## 11.2 Création du graphique

```python
plt.figure(figsize=(10,5))
plt.plot(values)
plt.xlabel("Temps")
plt.ylabel("Nombre de lignes")
plt.title("Évolution du nombre de lignes dans le flux")
plt.show()
```

---

# 12. Analyse du comportement du système

## Questions

1. Le graphique évolue-t-il de manière continue ou par paliers ?
2. Pourquoi ?
3. Quel lien avec le principe de micro-batch ?
4. Que se passe-t-il si plusieurs fichiers sont ajoutés simultanément ?

--- -->

# 12. Arrêt propre du streaming

```python
for q in spark.streams.active:
    print(f"Arrêt du stream : {q.name}")
    q.stop()

spark.stop()
```

---

# 13. Partie avancée — Streaming depuis l’API JCDecaux

## Objectif

Dans cette partie, vous devez construire un pipeline temps réel à partir de l’API JCDecaux.

Les données doivent être :

* récupérées régulièrement ;
* stockées dans un répertoire ;
* traitées automatiquement par Spark Streaming ;
* visualisées.

---

# 14. Présentation de l’API JCDecaux

Documentation :

```text
https://developer.jcdecaux.com/
```

Exemple d’URL :

```text
https://api.jcdecaux.com/vls/v1/stations?contract=Lyon&apiKey=VOTRE_CLE
```

---

