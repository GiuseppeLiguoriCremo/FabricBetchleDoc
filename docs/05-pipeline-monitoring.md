# `pipeline_monitoring_python.ipynb` — Journal des exécutions de pipelines (`log_pipelines`)

Notebook de **monitoring au niveau pipeline** (et non au niveau table comme `load_tracking`) : il est appelé en fin de pipeline pour enregistrer une ligne de log dans la table Delta **`Tables/dbo/log_pipelines`** d'un lakehouse de métadonnées centralisé (`LH_METADATA`).

> ℹ️ État actuel : ce notebook est encore **exploratoire** — il contient trois implémentations équivalentes de l'écriture du log (polars, DuckDB, pandas), toutes basées sur delta-rs. La version cible à conserver est l'une des trois ; les autres sont des variantes de comparaison. Il a été développé dans le workspace `CORE_DEV` (contexte distinct du POC M3).

## Paramètres (injectés par le pipeline appelant)

| Paramètre | Description |
|---|---|
| `pipeline_name` | Nom du pipeline (ex. `pl_m3_full`) |
| `workspace_name` | Nom du workspace (le notebook relit aussi le workspace réel depuis `notebookutils.runtime.context`) |
| `status` | Statut final du pipeline (`Succeeded`, `Failed`, …) |
| `start_time` / `end_time` | Timestamps ISO fournis par le pipeline (`@pipeline().TriggerTime`, etc.) |
| `duration` | Durée (secondes, ou millisecondes pour certains pipelines legacy — voir note ci-dessous) |
| `message` | Message d'erreur, vide si succès |
| `tasktype` | Catégorie libre (ex. `Pipeline for injections`) |

## Résolution de la cible

La table de log vit dans un **autre workspace/lakehouse** que celui du pipeline : les IDs sont résolus via la **variable library Fabric `ENV_VAR`** (`WORKSPACE_MONITORING_ID`, `LAKEHOUSE_METADATA_ID`), ce qui permet de pointer vers le bon `LH_METADATA` par environnement (DEV/PRD) sans toucher au code :

```
abfss://<WORKSPACE_MONITORING_ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_METADATA_ID>/Tables/dbo/log_pipelines
```

L'écriture traverse donc les workspaces en abfss complet, via **delta-rs** (`write_deltalake`, mode `append`).

## Schéma de `log_pipelines`

| Colonne | Type | Contenu |
|---|---|---|
| `Id` | int32 | Identifiant séquentiel : `MAX(Id) + 1` lu dans la table avant insertion (1 si table absente) |
| `Pipeline_name` | string | Nom du pipeline |
| `Task_type` | string | Catégorie |
| `Workspace_name` | string | Lu du contexte runtime (`currentWorkspaceName`) |
| `Status` | string | `🟢 Success` / `❌ Failed` (mapping depuis le statut pipeline) |
| `Description` | string | Message d'erreur éventuel |
| `DateID` | int32 | Date du log `YYYYMMDD` |
| `Start_time` / `End_time` | timestamp (tz UTC) | Parsés depuis les strings ISO du pipeline |
| `Duration` | string | `HH:MM:SS` |

### Points d'attention

- **Unité de durée** : la plupart des pipelines envoient des **secondes**, mais `pl_sap_master_PRD` envoie des **millisecondes** — la variante polars contient un switch sur le nom du pipeline. À homogénéiser côté pipelines si possible.
- **`Id` séquentiel non atomique** : le `MAX(Id)+1` est lu puis écrit sans verrou ; deux pipelines finissant au même instant peuvent produire un doublon d'`Id`. Acceptable pour du monitoring, à garder en tête si `Id` devait devenir une vraie clé.
- **Timestamps typés** : contrairement à `load_tracking`, cette table écrit de vrais timestamps (tz-aware UTC). C'est sans risque ici car **un seul moteur** (delta-rs) lit et écrit `log_pipelines` — la contrainte string-ISO de [03-load-tracking.md](03-load-tracking.md) ne s'applique qu'aux tables partagées delta-rs ↔ Spark. Si un jour Spark devait écrire dans `log_pipelines`, appliquer la même règle.

## Différence avec `load_tracking`

| | `load_tracking` | `log_pipelines` |
|---|---|---|
| Granularité | 1 ligne **par table et par étape** (loading / transform / retention) | 1 ligne **par run de pipeline** |
| Localisation | Lakehouse bronze du POC | `LH_METADATA` centralisé (cross-workspace) |
| Écrit par | Les 3 notebooks d'ingestion | Ce notebook, appelé en fin de pipeline |
| Usage | High-water mark incrémental + audit détaillé | Supervision globale des pipelines |
