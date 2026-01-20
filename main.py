from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

# --- INITIALISATION SPARK ---
# Remplace 'local[*]' par 'spark://localhost:7077' pour utiliser le cluster Docker
spark = SparkSession.builder \
    .appName("ELT_Achats_Spark") \
    .master("spark://localhost:7077") \
    .get_factory() \
    .getOrCreate()

start_time = time.time()

# --- 1. CHARGEMENT ---
# Spark infère le schéma automatiquement, mais on peut le forcer pour plus de vitesse
df = spark.read.csv("data/sources/achats.csv", header=True, inferSchema=True)

# --- 2. NETTOYAGE ---

# A & D. Supprimer NaNs sur colonnes critiques et erreurs de conversion
# En Spark, on filtre souvent directement après le cast
df_clean = df.dropna(subset=["id_achat", "montant"]) \
             .withColumn("montant", F.col("montant").cast("double")) \
             .withColumn("date_achat", F.to_timestamp("date_achat")) \
             .filter(F.col("montant").isNotNull() & F.col("date_achat").isNotNull())

# B. Doublons
df_clean = df_clean.dropDuplicates()

# E. Montants positifs
df_clean = df_clean.filter(F.col("montant") > 0)

# --- 3. CALCULS KPI ---
# On utilise l'agrégation Spark (beaucoup plus rapide sur gros volumes)
stats = df_clean.select(
    F.sum("montant").alias("ca_total"),
    F.avg("montant").alias("panier_moyen"),
    F.count("*").alias("nombre_achats"),
    F.countDistinct("id_client").alias("clients_uniques")
).collect()[0]

# Produit Star (Mode)
produit_star = df_clean.groupBy("produit").count() \
    .orderBy(F.desc("count")).first()["produit"]

end_time = time.time()

# --- AFFICHAGE ---
print(f"Traitement Spark terminé en : {end_time - start_time:.2f} secondes")
print(f"KPIs : CA={stats['ca_total']:.2f}, Panier Moyen={stats['panier_moyen']:.2f}")
print(f"Produit Star : {produit_star}")

# --- SAUVEGARDE ---
df_clean.write.mode("overwrite").csv("data/sources/achats_clean_spark.csv", header=True)