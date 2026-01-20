from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, mean as spark_mean, count, countDistinct
from pathlib import Path
from datetime import datetime
import time

# --- CONFIGURATION ---
base_path = Path(__file__).parent.parent / "data" / "sources"
input_path = str(base_path / "achats.csv")
output_path = str(base_path / "achats_clean_spark.csv")

# DÉMARRAGE DU CHRONOMÈTRE
start_time = time.time()

# --- CRÉATION DE LA SESSION SPARK ---
print("[PYSPARK] Initialisation de Spark...")
spark = SparkSession.builder \
    .appName("Pipeline_ELT_Achats") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# --- 1. CHARGEMENT ---
print(f"[PYSPARK] Chargement du fichier : {input_path}")
df_achats = spark.read.csv(input_path, header=True, inferSchema=True)
initial_count = df_achats.count()
print(f"Nombre de lignes initiales : {initial_count}")

# --- 2. NETTOYAGE ---

# A. Supprimer les lignes avec valeurs manquantes critiques
df_achats = df_achats.dropna(subset=['id_achat', 'montant'])
after_na_count = df_achats.count()
print(f"Après suppression des NaNs critiques : {after_na_count}")

# B. Supprimer les doublons
df_achats = df_achats.dropDuplicates()
after_dup_count = df_achats.count()
print(f"Après suppression des doublons : {after_dup_count}")

# C. Conversion de types et nettoyage
# Cast montant en double (équivalent de numeric)
df_achats = df_achats.withColumn("montant", col("montant").cast("double"))

# Cast date_achat en timestamp
df_achats = df_achats.withColumn("date_achat", col("date_achat").cast("timestamp"))

# D. Supprimer les lignes où la conversion a échoué (NULL)
df_achats = df_achats.dropna(subset=['montant', 'date_achat'])

# E. Filtrer les montants positifs
df_achats = df_achats.filter(col("montant") > 0)
final_count = df_achats.count()
print(f"Nombre de lignes après nettoyage final : {final_count}")

# --- 3. SAUVEGARDE ---
# Coalesce(1) pour avoir un seul fichier CSV
df_achats.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# --- 4. CALCUL DES KPI ---
# Recharger pour calculer les KPI
df_clean = spark.read.csv(output_path, header=True, inferSchema=True)

# Calculs des KPI
ca_total = df_clean.agg(spark_sum("montant")).collect()[0][0]
panier_moyen = df_clean.agg(spark_mean("montant")).collect()[0][0]
nombre_achats = df_clean.count()
clients_uniques = df_clean.agg(countDistinct("id_client")).collect()[0][0]

# Produit le plus fréquent
produit_star = df_clean.groupBy("produit") \
    .count() \
    .orderBy(col("count").desc()) \
    .first()["produit"]

kpi = {
    "ca_total": round(float(ca_total), 2),
    "panier_moyen": round(float(panier_moyen), 2),
    "nombre_achats": int(nombre_achats),
    "clients_uniques": int(clients_uniques),
    "produit_star": produit_star
}

metadata = {
    "file_info": {
        "source": "achats.csv",
        "cleaning_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": nombre_achats
    },
    "kpis": kpi
}

# ARRÊT DU CHRONOMÈTRE
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "="*50)
print("RÉSULTATS PYSPARK")
print("="*50)
print(f"⏱️  TEMPS D'EXÉCUTION : {execution_time:.4f} secondes")
print("\nKPIs:")
for k, v in kpi.items():
    print(f"  • {k}: {v}")
print("="*50)

# Arrêter Spark
spark.stop()