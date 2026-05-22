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
cd /workspaces/spark-streaming/spark-streaming
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

* **`.builder`** : Active l'interface de configuration.
* **`.appName(...)`** : Donne un nom à votre application pour l'identifier dans les logs et le Spark UI.
* **`.master(...)`** : Indique l'adresse du cluster (le "maître") où le code doit s'exécuter.
* **`.config("spark.sql.shuffle.partitions", "4")`** : Fixe à 4 le nombre de partitions pour les opérations de mélange de données (optimisation pour jeux de données légers).
* **`.getOrCreate()`** : Lance la session ou récupère une instance existante.

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

* Performance (Lecture optimisée) : Sans schéma, Spark doit scanner tout le fichier pour "deviner" (inférer) les types, ce qui ralentit considérablement le chargement des données.

* Fiabilité (Typage strict) : Cela garantit que chaque colonne contient le bon type (ex: LongType pour les nombres). Cela évite les erreurs de typage inattendues ou des colonnes lues comme du texte par défaut.

* Gestion des valeurs manquantes : Le troisième argument de StructField (ici True) indique que la colonne accepte des valeurs nulles (nullable). Cela permet à Spark de gérer proprement les données manquantes.

* Format sans en-tête : C'est indispensable si vous travaillez avec des fichiers (comme des CSV) qui ne contiennent pas de ligne d'en-tête, car Spark ne saurait pas comment nommer ou typer les colonnes autrement.
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

```bash
# On définit uniquement la source du streaming
raw_stream = (spark.readStream
              .format("csv")
              .schema(schema_defined)
              .option("header", "true")
              .option("maxFilesPerTrigger", 1)
              .load(STREAM_PATH))

```

```bash
# On prend le flux préparé et on définit la destination (le "sink")
stream_memory_query = (raw_stream.writeStream
                       .outputMode("append")
                       .format("memory")
                       .queryName("stream_data_check")
                       .trigger(processingTime="5 seconds")
                       .start())

```
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



**1. Pourquoi les données n'apparaissent pas immédiatement ?**
Parce que Spark fonctionne par **micro-batchs**. Il ne lit pas les fichiers en continu seconde par seconde, mais attend que son cycle de traitement (le *trigger*) se déclenche pour aller vérifier s'il y a de nouveaux fichiers.

**2. Quel est le rôle de `maxFilesPerTrigger` ?**
C'est un **limiteur de vitesse**. Il empêche Spark d'essayer de traiter trop de fichiers d'un coup, ce qui protège la mémoire du cluster en cas d'arrivée massive de données.

**3. Différence entre batch et micro-batch ?**

* **Batch :** On traite un bloc de données fixe une seule fois (fin de traitement = fin du programme).
* **Micro-batch :** Le programme tourne en continu. À chaque intervalle, il traite seulement les *nouveaux* fichiers arrivés depuis le dernier cycle.


---

# 7. Suppression d’un fichier pendant le streaming

## Expérience

Pendant que le stream tourne :

1. Supprimer un fichier déjà traité.
2. Observer le comportement du flux.


---

## Questions

Voici les réponses en version courte :

**1. Les données disparaissent-elles ?**
**Non.** Le streaming est en mode "ajout" (`append`). Une fois traitées, les données sont stockées dans la destination (la mémoire), indépendamment du fichier source.

**2. Pourquoi ?**
Parce que Spark traite le flux comme une suite d'événements à sens unique. Une fois transformée, la donnée est considérée comme un résultat final.

**3. Spark relit-il les anciens fichiers ?**
**Non.** Il est conçu pour être efficace et ne traite chaque donnée qu'une seule fois.

**4. Comment Spark mémorise-t-il les fichiers déjà traités ?**
Il utilise un **journal interne (checkpoint)**. Il garde une trace des fichiers déjà lus (leurs "offsets") pour savoir ce qu'il a déjà validé.

**5. Que se passe-t-il si on remet le fichier supprimé ?**
**Rien du tout.** Spark reconnaît le nom du fichier dans son journal et l'ignore, car il le considère comme "déjà traité". Pour qu'il le reprenne, il faudrait le renommer.

C'est plus clair pour toi comme ça ?
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

stream_df = (
    spark.readStream
    .format("csv")
    .schema(schema_defined)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .load(STREAM_PATH)
)






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

Voici les réponses en version courte :

**Pourquoi le mode `complete` ?**
Parce que tu fais un **`groupBy`**. Comme Spark doit recalculer le total à chaque nouveau batch, le mode `complete` lui dit : "Affiche-moi le résultat total actuel à chaque mise à jour".

**Différence avec `append` :**

* **`append`** : Ajoute seulement les *nouvelles lignes* arrivées (pour stocker des événements bruts).
* **`complete`** : Réécrit la *table entière* avec les agrégations à jour (pour suivre des totaux/statistiques).

C'est plus clair ?
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


# 1. Nombre de personnes par sexe
```bash
sex_df = stream_df.groupBy("sex").count()
```

# 2. Nombre de personnes par pays (native-country)
```bash
country_df = stream_df.groupBy("native-country").count()
```


# 3. Moyenne des heures travaillées
```bash
hours_df = stream_df.agg({"hours-per-week": "avg"})
```


# 4. Moyenne d'âge par profession (occupation)
```bash
age_occupation_df = stream_df.groupBy("occupation").avg("age")
```


# Exemple pour lancer un stream (remplacez le dataframe et le nom)
```bash
query = sex_df.writeStream.format("console").outputMode("complete").start()
```

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

```bash
import requests
import json
import time
import os
from datetime import datetime

API_KEY = "clé api" # Remplacez par votre clé API
CONTRACT = "Lyon"
URL = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT}&apiKey={API_KEY}"
OUTPUT_DIR = "./data_streaming/" # Le dossier que Spark va surveiller

# Créer le dossier s'il n'existe pas
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Démarrage de la récupération des données JCDecaux...")

while True:
    try:
        response = requests.get(URL)
        if response.status_code == 200:
            stations = response.json()
            
            # Nom de fichier unique basé sur le timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(OUTPUT_DIR, f"stations_{timestamp}.json")
            
            # Écrire un objet JSON par ligne (idéal pour Spark)
            with open(filename, 'w', encoding='utf-8') as f:
                for station in stations:
                    f.write(json.dumps(station) + '\n')
                    
            print(f"[{timestamp}] Données sauvegardées : {filename}")
        else:
            print(f"Erreur API : {response.status_code}")
            
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        
    # Attendre 1 minute avant le prochain appel (l'API JCDecaux se met à jour environ toutes les minutes)
    time.sleep(60)
```
```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, round

# 1. Initialiser la session Spark
spark = SparkSession.builder \
    .appName("JCDecaux_Streaming") \
    .getOrCreate()

# 2. Définir le schéma des données JSON attendues
# (Basé sur la documentation JCDecaux, simplifié ici pour les champs essentiels)
schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("status", StringType(), True)
])

# 3. Lire le flux de données (surveiller le dossier)
streaming_df = spark.readStream \
    .schema(schema) \
    .json("./data_streaming/")

# 4. Traitement des données : par exemple, calculer le taux de remplissage et filtrer les stations ouvertes
processed_df = streaming_df \
    .filter(col("status") == "OPEN") \
    .withColumn("taux_remplissage_pct", round((col("available_bikes") / col("bike_stands")) * 100, 2)) \
    .select("number", "name", "available_bikes", "available_bike_stands", "taux_remplissage_pct")

# 5. Démarrer le flux et afficher les résultats dans la console (pour tester)
# "append" ajoute les nouvelles lignes, "complete" affiche tout le tableau à chaque mise à jour (nécessite une agrégation)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
```


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

