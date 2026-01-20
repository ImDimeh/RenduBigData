import subprocess
import time

print("="*60)
print("BENCHMARK : PANDAS vs PYSPARK")
print("="*60)

# Test Pandas
print("\n🐼 Exécution PANDAS...")
start = time.time()
subprocess.run(["python", "scripts/pandas_pipeline.py"])
pandas_time = time.time() - start

# Test PySpark
print("\n⚡ Exécution PYSPARK...")
start = time.time()
subprocess.run(["python", "scripts/pyspark_pipeline.py"])
spark_time = time.time() - start

# Résultats
print("\n" + "="*60)
print("COMPARAISON FINALE")
print("="*60)
print(f"Pandas  : {pandas_time:.4f} secondes")
print(f"PySpark : {spark_time:.4f} secondes")
speedup = pandas_time / spark_time if spark_time > 0 else 0
print(f"Speedup : {speedup:.2f}x")
if speedup > 1:
    print(f"✅ PySpark est {speedup:.2f}x plus rapide")
else:
    print(f"⚠️ Pandas est {1/speedup:.2f}x plus rapide (normal sur petit dataset)")
print("="*60)