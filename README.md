# Documentation — Ingestion M3 dans Microsoft Fabric (Cremo)

Cette documentation décrit la chaîne d'ingestion des données **Infor M3** vers le lakehouse bronze (`lh_bronze`) dans Microsoft Fabric, mise en place pour Cremo par Bechtle.

## Vue d'ensemble

```
Infor M3 (ION API / EXPORTMI)
        │
        │  1. Extraction REST paginée
        ▼
┌─────────────────────────────┐
│  m3_loading_tables.ipynb    │   Notebook Python pur (pandas + requests + delta-rs)
│  "Step = loading"           │
└─────────────────────────────┘
        │
        │  Parquet partitionné par date
        ▼
Files/M3/<TABLE>/load_date=YYYY-MM-DD/<run_id>_<date>.parquet
        │
        │  2. Transformation / chargement Delta
        ▼
┌─────────────────────────────┐
│  files_to_delta.ipynb       │   Notebook PySpark
│  "Step = transform"         │
└─────────────────────────────┘
        │
        ▼
Tables/dbo/tbl_m3_<TABLE>     (table Delta bronze, partitionnée par _load_date)

        + Tables/dbo/load_tracking   (journal des exécutions, partagé par les 2 notebooks)
```

Les deux notebooks sont orchestrés par le pipeline Fabric **`pl_m3_full`** :

1. **Filter actives** : filtre la liste des tables (paramètre `m3_table_name`, un tableau JSON — voir `table_m3.json`) sur `isactive == "True"`.
2. **ForEach** (parallélisme `batchCount = 2`) : pour chaque table active, exécute séquentiellement :
   - `Notebook1_loading` → `m3_loading_tables.ipynb` (extraction M3 → parquet dans `Files/`) ;
   - `Notebook2_transform` → `files_to_delta.ipynb` (parquet → table Delta), exécuté **uniquement si** le loading a réussi (`dependsOn: Succeeded`), avec le tag de session Spark `m3_spark`.

Les deux notebooks reçoivent les **mêmes paramètres** depuis le pipeline (`TABLE_NAME`, `CURSOR_COL`, `PK_COLS`, `LOADING_MODE`, `INCREMENTAL_COL`, `SOURCE_NAME`, `RUN_ID`), ce qui permet de corréler les deux étapes d'un même run via `Run_id` dans `load_tracking`.

## Configuration des tables (`table_m3.json`)

Chaque table M3 à ingérer est décrite par un objet :

| Champ | Exemple | Rôle |
|---|---|---|
| `table_name` | `CINACC` | Nom de la table M3 (API EXPORTMI) |
| `loading_mode` | `full` ou `incremental` | Stratégie de chargement |
| `cursor_col` | `EZANBR` | Colonne numérique croissante utilisée pour **paginer** l'extraction |
| `incremental_col` | `EZLMDT` | Colonne date (format `YYYYMMDD`) utilisée pour la **fenêtre incrémentale** (requis si `incremental`) |
| `pk_cols` | `["EZCONO","EZDIVI","EZANBR","EZSENO"]` | Clé primaire : déduplication à l'extraction + calcul du `hkey` en bronze |
| `source_name` | `m3` | Préfixe du dossier dans `Files/` |
| `isactive` | `"True"` | Permet de désactiver une table sans la supprimer de la config |

## Documentation détaillée

- [docs/01-m3-loading-tables.md](docs/01-m3-loading-tables.md) — extraction M3 → parquet (pagination, fenêtre incrémentale, idempotence)
- [docs/02-files-to-delta.md](docs/02-files-to-delta.md) — parquet → tables Delta bronze (modes full/incremental, hkey, schéma)
- [docs/03-load-tracking.md](docs/03-load-tracking.md) — la table `load_tracking` : schéma et contrainte de compatibilité delta-rs / Spark
