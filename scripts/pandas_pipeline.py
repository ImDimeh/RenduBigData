import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# --- CONFIGURATION DES CHEMINS ---
base_path = Path(__file__).parent.parent / "data" / "sources"
input_path = base_path / "achats.csv"
output_path = base_path / "achats_clean_pandas.csv"

# DÉMARRAGE DU CHRONOMÈTRE
start_time = time.time()

# --- 1. CHARGEMENT ---
print(f"[PANDAS] Chargement du fichier : {input_path}")
df_achats = pd.read_csv(input_path, sep=',')
print(f"Nombre de lignes initiales : {len(df_achats)}")

# --- 2. NETTOYAGE ---
# A. Supprimer les lignes complètement vides ou avec des valeurs manquantes critiques
df_achats.dropna(subset=['id_achat', 'montant'], inplace=True)
print(f"Après suppression des NaNs critiques : {len(df_achats)}")

# B. Supprimer les doublons
df_achats.drop_duplicates(inplace=True)
print(f"Après suppression des doublons : {len(df_achats)}")

# C. Conversion de types
df_achats['montant'] = pd.to_numeric(df_achats['montant'], errors='coerce')
df_achats['date_achat'] = pd.to_datetime(df_achats['date_achat'], errors='coerce')

# D. Supprimer les lignes où la conversion a échoué
df_achats.dropna(subset=['montant', 'date_achat'], inplace=True)

# E. Supprimer les montants incohérents
df_achats = df_achats[df_achats['montant'] > 0]
print(f"Nombre de lignes après nettoyage final : {len(df_achats)}")

# --- 3. SAUVEGARDE ---
df_achats.to_csv(output_path, index=False, encoding='utf-8')

# --- 4. CALCUL DES KPI ---
df = pd.read_csv(output_path)
kpi = {
    "ca_total": round(df['montant'].sum(), 2),
    "panier_moyen": round(df['montant'].mean(), 2),
    "nombre_achats": int(df.shape[0]),
    "clients_uniques": int(df['id_client'].nunique()),
    "produit_star": df['produit'].mode()[0]
}

metadata = {
    "file_info": {
        "source": "achats.csv",
        "cleaning_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": len(df)
    },
    "kpis": kpi,
    "schema": {
        "columns": list(df.columns),
        "types": df.dtypes.astype(str).to_dict()
    }
}

# ARRÊT DU CHRONOMÈTRE
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "="*50)
print("RÉSULTATS PANDAS")
print("="*50)
print(f"⏱️  TEMPS D'EXÉCUTION : {execution_time:.4f} secondes")
print("\nKPIs:")
for k, v in kpi.items():
    print(f"  • {k}: {v}")
print("="*50)