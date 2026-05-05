# import os
# import sys

from pyspark.sql import SparkSession

# --- AJOUT CRUCIAL POUR WINDOWS ---
# Cela dit à PySpark d'utiliser l'exécutable Python actuel au lieu de chercher "python3"
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# SOUCIS REGLÉ AVEC LES VARIABLES D'ENVIRONNEMENTS
# Créer une session Spark
spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# Créer un DataFrame simple
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Afficher le DataFrame
df.show()

# Fermer la session Spark  
spark.stop()