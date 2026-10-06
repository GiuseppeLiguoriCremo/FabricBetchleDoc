# `files_to_delta.ipynb` — Parquet (`Files/`) → Tables Delta bronze

**Étape `transform`** du pipeline. Ce notebook **PySpark** lit les partitions parquet déposées par `m3_loading_tables.ipynb` dans `Files/` et les charge dans la table Delta bronze `Tables/<schema>/tbl_m3_<TABLE>`. Il s'exécute uniquement si l'étape de loading a réussi.

Spark est nécessaire ici : les tables `tbl_m3_*` peuvent être volumineuses, et les mécaniques `replaceWhere` / `mergeSchema` / partitionnement Delta sont du ressort de Spark.

## Paramètres

| Paramètre | Requis | Description |
|---|---|---|
| `TABLE_NAME` | oui | Table M3 (ex. `CINACC`) → cible `tbl_m3_<TABLE_NAME>` |
| `PK_COLS` | oui | Liste (ou string JSON) des colonnes PK, pour le calcul du `hkey` |
| `LOADING_MODE` | oui | `full` ou `incremental` — stratégie d'écriture Delta |
| `CURSOR_COL`, `INCREMENTAL_COL` | — | **Acceptés mais ignorés** (compatibilité : le pipeline envoie les mêmes paramètres aux deux notebooks) |
| `TARGET_SCHEMA` | non | Défaut `dbo` |
| `SOURCE_NAME` | non | Défaut `M3`. Cas particulier `gupta_<x>` : le dossier source devient `Files/gupta/<x>/<TABLE>` |
| `TRACKING_PATH` | non | Défaut `Tables/dbo/load_tracking` ; `None` explicite désactive le tracking |
| `RUN_ID` | non | UUID pipeline ; généré localement si absent |

Les chemins sont **relatifs** (`Files/...`, `Tables/...`) et résolus par Spark via le lakehouse attaché au notebook — contrairement à `m3_loading_tables` qui construit des chemins abfss complets.

## Configuration Spark notable

```python
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
```

Les données M3 contiennent des dates sentinelles anciennes (`1900-01-01`, `0001-01-01`). Sans ces options, Spark 3.x refuse de lire ces timestamps (`INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME`). `CORRECTED` lit les valeurs brutes en calendrier grégorien proleptique.

## Déroulement

### 1. Inventaire des partitions source

`list_source_dates` liste les sous-dossiers `load_date=YYYY-MM-DD` de `Files/<SOURCE>/<TABLE>`. Aucune partition → `RuntimeError` (le loading aurait dû en produire au moins une, ou la table n'a jamais été chargée).

### 2. Normalisation à la lecture

Chaque partition est lue par `read_partition_normalized`, qui caste **toutes les colonnes métier en string** (les colonnes de lineage `_load_date`, `_run_id`, `_ingested_at` gardent leur type). Le bronze est donc intégralement string : c'est voulu, le typage se fait en silver. Cela neutralise aussi les dérives de schéma entre partitions (une colonne inférée int un jour et string le lendemain).

Plusieurs partitions sont combinées par `unionByName(allowMissingColumns=True)` : si une colonne apparaît ou disparaît côté M3 entre deux dates, l'union remplit avec des nulls au lieu d'échouer.

### 3. Clé de hachage `hkey`

`add_hkey` ajoute en première colonne un **SHA-256 de la concaténation des colonnes PK** (`concat_ws("||", ...)`, valeurs trimées, null → chaîne vide). Ce `hkey` sert de clé technique stable pour les couches aval (comparaisons, merge en silver). Si `PK_COLS` est vide ou ne contient que des chaînes vides, aucune colonne n'est ajoutée (signalé dans le log d'exécution).

### 4. Écriture Delta — selon `LOADING_MODE`

La cible est toujours **partitionnée par `_load_date`**.

#### Mode `full` — snapshots historisés

Seule la **dernière** partition source est chargée (le full du jour). Deux cas :

- **Table existante** : `overwrite` avec `replaceWhere _load_date = '<date>'` + `mergeSchema` → seul le snapshot de cette date est remplacé, **les snapshots des dates précédentes sont conservés**. La table full est donc un historique de snapshots complets, un par date de chargement (`mode_log = full_snapshot`).
- **Première exécution** : création de la table (`overwrite` + `overwriteSchema`), partitionnée par `_load_date` pour que les `replaceWhere` suivants soient efficaces (`mode_log = full_init`).

Relancer le même jour est idempotent : le snapshot du jour est remplacé, pas dupliqué.

#### Mode `incremental` — append des nouvelles dates

1. Lecture des `_load_date` **distinctes déjà présentes** dans la table Delta cible.
2. `new_dates` = partitions source absentes de la cible. La détection du delta se fait donc **par comparaison source/cible**, pas via `load_tracking` : si un run transform a échoué après un loading réussi, la partition manquante est automatiquement rattrapée au run suivant.
3. Aucune nouvelle date → le run se termine en Success sans écrire (`mode_log = skip`).
4. Sinon, union des nouvelles partitions, `hkey`, puis :
   - table existante → `append` + `mergeSchema` (`mode_log = append`) ;
   - première exécution → création `overwrite` + `overwriteSchema` (`mode_log = init`).

> ⚠️ Le mode incremental est **append-only au niveau fichier** : la déduplication fine (une même PK modifiée sur deux dates différentes apparaît deux fois, une par `_load_date`) est assumée en bronze et résolue en silver via le `hkey` / la PK.

### 5. Tracking

Dans le bloc `finally` (sauf si `TRACKING_PATH = None`), une ligne est ajoutée à `load_tracking` avec `Step = "transform"`, le `mode_log` effectif (`full_snapshot`, `full_init`, `append`, `init`, `skip`), le statut, le nombre de lignes écrites et la durée. Les colonnes propres au loading (`Batches`, `Cursor_col`, fenêtre…) sont à 0/null : le schéma est **identique** à celui écrit par `m3_loading_tables`, condition pour que les deux notebooks partagent la même table.

L'écriture se fait via **Spark** (pas delta-rs : delta-rs échoue en notebook Spark Fabric sur une erreur de certificat), avec un **schéma explicite** et les timestamps en **string ISO**. Ce choix est critique pour la compatibilité du protocole Delta — détails dans [03-load-tracking.md](03-load-tracking.md).
