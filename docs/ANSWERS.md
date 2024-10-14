# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_
### Étape 1: Description de la solution
La solution implémentée est un pipeline d'ingestion et de traitement de données conçu pour récupérer les données à partir d'une API, effectuer les transformations via un processus ETL (Extract, Transform, Load), et sauvegarder les résultats sous forme de fichiers CSV. Le pipeline est optimisé pour l'exécution distribuée grâce à l'utilisation de PySpark pour les transformations de données et Ray pour l'ingestion parallèle des données.

L'ingestion des données est réalisée en parallèle à l'aide de Ray, et les transformations des données sont effectuées en tirant parti des capacités distribuées de PySpark. Le pipeline s'adapte dynamiquement aux ressources CPU disponibles sur la machine.

### Étape 2: Installation
Installer Conda : Si Conda n'est pas déjà installé sur votre machine, vous pouvez télécharger et installer Miniconda à partir du site officiel :
Télécharger Miniconda

Créer et activer un environnement Conda :

#### conda create --name etl_pipeline python=3.9 

#### conda activate etl_pipeline

Installer Java (nécessaire pour PySpark) avec apt :

PySpark nécessite une installation de Java JDK. Vous pouvez installer Java sur Ubuntu ou Debian avec apt comme suit :

#### sudo apt update

#### sudo apt install openjdk-8-jdk

#### java -version

Une fois l'environnement activé et Java installé, installez toutes les dépendances requises pour le projet en exécutant :

#### pip install -r requirements.txt
### pip install -e .

Ce fichier requirements.txt contient les bibliothèques nécessaires telles que PySpark, Ray, pytest, et d'autres.

Structure des composants :
Les composants principaux sont organisés en modules distincts :

DataIngestorPipeline : Ingestion de données depuis l'API FastAPI, avec parallélisation à l'aide de Ray.

DataLoader : Chargement des fichiers CSV dans des DataFrames Spark pour traitement.

DataTransformer : Transformations des données incluant le nettoyage, les jointures et la normalisation.

DataWriter : Sauvegarde des résultats transformés sous forme de fichiers CSV.

Tests : Les tests unitaires sont écrits en pytest et couvrent les étapes principales du pipeline.

### Étape 3: Comment exécuter le pipeline

Exécution du pipeline complet :

Ce pipeline récupère les données des endpoints /tracks, /users, et /listen_history de l'API, les sauvegarde en fichiers CSV, puis exécute le pipeline ETL pour transformer les données.

python main_run.py

Le script main.py contient le flux principal du pipeline d'ingestion et de ETL. Voici les étapes incluses :

L'ingestion des données à partir de l'API FastAPI.
Le chargement des données en DataFrames Spark.
L'application des transformations nécessaires sur les données.
La sauvegarde des résultats finaux dans des fichiers CSV.

Exécution des tests :
Vous pouvez exécuter les tests unitaires qui valident chaque étape du pipeline :

### pytest test/

Les tests couvriront l'ingestion, la transformation et l'écriture des données en utilisant des DataFrames mockés pour simuler le comportement du pipeline. Les résultats des tests vous permettront de vérifier que chaque composant du pipeline fonctionne correctement.

## Questions (étapes 4 à 7)

### Étape 4

Comment gérez-vous l'ingestion de données dans cette solution ?
L'ingestion de données est gérée via le DataIngestorPipeline, qui interroge les endpoints d'une API FastAPI pour récupérer les données de manière paginée. Chaque type de données (par exemple, /tracks, /users, /listen_history) est traité séparément. Grâce à Ray, l'ingestion est distribuée et parallélisée, ce qui permet de répartir le travail sur plusieurs workers, chacun utilisant un CPU disponible. De plus l utilisation d'un generateur pour les problem de memoire.

Les données ingérées sont ensuite sauvegardées sous forme de fichiers CSV, ce qui facilite leur traitement dans le pipeline ETL.

### Étape 5
Comment les transformations des données sont-elles appliquées ?

Les transformations de données sont gérées dans le module DataTransformer. Une fois que les données sont chargées à partir des fichiers CSV par le DataLoader, les transformations suivantes sont appliquées :

Nettoyage des données : Les lignes contenant des valeurs nulles dans des colonnes critiques telles que name, artist, et duration sont supprimées.
Jointures : Les tables d'utilisateurs, d'historique d'écoute et de titres musicaux sont jointes pour créer un DataFrame consolidé.
Normalisation : Les colonnes sont renommées et mises en cohérence à travers les différents jeux de données.
Ces transformations sont appliquées de manière distribuée grâce à PySpark, ce qui permet d'assurer une haute performance même sur des volumes importants de données._

### Étape 6

Comment gérez-vous l'autoscaling et l'optimisation des ressources ?
Le pipeline est conçu pour s'adapter dynamiquement aux ressources disponibles sur la machine. Voici les deux principales méthodes d'optimisation :

Autoscaling des CPUs : Le pipeline utilise multiprocessing.cpu_count() pour détecter automatiquement le nombre de CPUs disponibles sur la machine et configure PySpark pour tirer parti de ces ressources, répartissant ainsi les tâches de transformation sur tous les cœurs disponibles.

Parallélisation avec Ray : L'ingestion de données est parallélisée avec Ray, qui permet de créer plusieurs workers pour traiter les pages d'API en parallèle, réduisant ainsi le temps nécessaire à l'ingestion des données. Cela garantit une utilisation efficace des ressources, quel que soit l'environnement d'exécution.

### Étape 7

Le pipeline est testé avec pytest et des mocks pour simuler l'exécution des différentes étapes :

DataIngestorPipeline : Testé en simulant les appels API et en utilisant des mocks pour vérifier que les données sont bien récupérées et stockées dans les fichiers CSV.

DataLoader : Vérifie que les fichiers CSV sont correctement chargés dans les DataFrames Spark, en s'assurant que ceux-ci ne sont pas vides.

DataTransformer : Teste les transformations, y compris les jointures et le nettoyage des données, pour garantir que les données sont correctement préparées pour les étapes ultérieures.

DataWriter : Utilise des mocks pour s'assurer que les données transformées sont correctement écrites dans des fichiers CSV.

Ces tests sont configurés pour garantir que chaque étape du pipeline fonctionne comme prévu.




