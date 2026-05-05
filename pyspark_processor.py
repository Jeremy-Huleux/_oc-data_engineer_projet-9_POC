from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

# 1. Initialisation de Spark avec le connecteur Kafka
# ATTENTION : La version 3.5.0 ici doit correspondre à la version de PySpark.
spark = SparkSession.builder \
    .appName("TicketProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Pour éviter que la console soit polluée par des logs d'information inutiles
spark.sparkContext.setLogLevel("WARN")

# 2. Définition stricte du schéma de nos tickets
ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("request", StringType(), True),
    StructField("type", StringType(), True),
    StructField("priority", StringType(), True)
])

print("Démarrage du Stream PySpark...")

# 3. Connexion à Redpanda (qui utilise l'API Kafka)
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Nettoyage : Binaire -> String -> JSON structuré
# On récupère la colonne 'value' (le message) et on la décode
df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_value")

# On applique le schéma pour créer de vraies colonnes Spark
df_parsed = df_string.select(from_json(col("json_value"), ticket_schema).alias("data")).select("data.*")

# 5. Transformation Métier : Assignation d'une équipe selon le type de demande
df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("type") == "Technique", "Support IT Niveau 2")
    .when(col("type") == "Facturation", "Service Comptabilité")
    .otherwise("Service Client Général")
)

# 6. Exportation des résultats (Sink) vers des fichiers Parquet
# On remplace l'affichage console par une écriture sur le disque
query = df_enriched.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./output_data/tickets") \
    .option("checkpointLocation", "./checkpoints/tickets") \
    .start()

# Empêche le script de s'arrêter
query.awaitTermination()