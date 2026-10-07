# `retention_partitions.ipynb` — Purge des anciennes partitions

**Étape `retention`**. Notebook **PySpark** (aucune dépendance pip) qui applique une politique de rétention sur les données bronze d'une table : suppression des vieilles partitions `_load_date` dans la table Delta `tbl_m3_<TABLE>` **et** des vieux dossiers parquet dans `Files/`.

Il est orchestré par le pipeline **`pl_m3_retention`** : même structure que `pl_m3_full` (Filter `isactive` → ForEach), mais en **séquentiel** (`isSequential: true`) et avec seulement 4 paramètres passés au notebook : `TABLE_NAME`, `RETENTION_DAYS` (actuellement **90 jours**, en dur dans le pipeline), `LOADING_MODE` et `RUN_ID`.

## Paramètres

| Paramètre | Requis | Défaut | Description |
|---|---|---|---|
| `TABLE_NAME` | oui | — | Table concernée (ex. `CINACC`) |
| `RETENTION_DAYS` | oui | — | Rétention par défaut (jours), pour Delta **et** Files |
| `LOADING_MODE` | non | `full` | Active le garde-fou incrémental (voir ci-dessous) |
| `FORCE_DELTA_RETENTION` | non | `False` | Force la purge Delta même en incrémental |
| `PURGE_FILES_INCREMENTAL` | non | `False` | Autorise la purge des parquets même en incrémental |
| `RETENTION_DAYS_DELTA` / `RETENTION_DAYS_FILES` | non | `RETENTION_DAYS` | Overrides séparés Delta / Files |
| `TARGET_SCHEMA` | non | `dbo` | Schéma de la table cible |
| `VACUUM_HOURS` | non | `168` | Rétention VACUUM (plancher Fabric = 7 jours) |
| `TRACKING_PATH` | non | `Tables/dbo/load_tracking` | `None` désactive le tracking |
| `RUN_ID`, `SOURCE_NAME` | non | UUID local / `M3` | Corrélation et tracking |

Les booléens acceptent les strings du pipeline (`"true"`, `"1"`, `"yes"`…).

## ⚠️ Garde-fou : les tables incrémentales sont protégées

C'est la règle centrale du notebook :

```
do_delta = (loading_mode == "full") OU FORCE_DELTA_RETENTION
do_files = (loading_mode == "full") OU PURGE_FILES_INCREMENTAL
```

**Pourquoi :** sur une table **full**, chaque `_load_date` est un snapshot complet — supprimer les vieux snapshots ne perd rien, la dernière date contient tout. Sur une table **incrémentale**, chaque `_load_date` contient *uniquement* les enregistrements modifiés ce jour-là : un enregistrement jamais re-modifié n'existe **que** dans sa partition d'origine. Purger les vieilles partitions d'une incrémentale = **perdre l'état courant** de ces enregistrements.

Par défaut, une table incrémentale n'est donc **jamais purgée** (ni Delta ni Files) ; il faut les overrides explicites `FORCE_DELTA_RETENTION` / `PURGE_FILES_INCREMENTAL` pour passer outre, en connaissance de cause.

## Déroulement

Les cutoffs sont calculés en UTC : on **conserve** `_load_date >= aujourd'hui − RETENTION_DAYS[_DELTA|_FILES]`.

### 1. Table Delta (`Tables/<schema>/tbl_m3_<TABLE>`)

Si la purge Delta est autorisée et que la table existe :

1. `COUNT(*)` des lignes avec `_load_date < cutoff` (pour le log) ;
2. si > 0 : `DELETE FROM delta.`…`` WHERE _load_date < cutoff` puis `VACUUM … RETAIN <VACUUM_HOURS> HOURS` pour libérer physiquement les fichiers (défaut 168 h, le minimum autorisé par Fabric).

Le DELETE étant aligné sur la colonne de partition `_load_date`, il se traduit en suppression de partitions entières (efficace, pas de réécriture de fichiers).

### 2. Parquets bronze (`Files/`)

Si la purge Files est autorisée : liste des dossiers `load_date=YYYY-MM-DD` et `notebookutils.fs.rm` récursif de chaque dossier dont la date est antérieure au cutoff (comparaison lexicographique, valide sur le format ISO).

> ⚠️ **Limitation connue** : le notebook liste `Files/<TABLE_NAME>` **sans préfixe source**, alors que la chaîne d'ingestion écrit sous `Files/<SOURCE_NAME>/<TABLE_NAME>` (ex. `Files/m3/CINACC`). En l'état, la purge Files ne trouve donc pas les parquets M3 (le `ls` échoue silencieusement → 0 partition supprimée). À corriger en alignant `FILES_BASE` sur la convention `Files/{source_name}/{table_name}` si la purge des parquets est souhaitée.

## Tracking

Même mécanique que `files_to_delta` (bloc `finally`, écriture **Spark**, schéma explicite, timestamps **string ISO** — voir [03-load-tracking.md](03-load-tracking.md)), avec `Step = "retention"` et un `Mode` reflétant ce qui a réellement tourné :

- `retention` — purge Delta effectuée (± Files) ;
- `retention_files` — seuls les parquets ont été purgés ;
- `retention_skip` — rien n'a été purgé (table incrémentale sans override).

Réutilisation des colonnes du schéma commun : `Total_records` = lignes Delta supprimées, `Batches` = dossiers parquet supprimés.
