# `m3_loading_tables.ipynb` — Extraction M3 → Parquet (`Files/`)

**Étape `loading`** du pipeline. Ce notebook extrait une table M3 via l'API REST **EXPORTMI** et écrit le résultat en **parquet partitionné par date de chargement** dans la zone `Files/` du lakehouse bronze.

## Caractéristique importante : Python pur, pas de Spark

Ce notebook est volontairement un notebook **Python pur** (pandas + requests + delta-rs). Il ne démarre **aucune session Spark** :

- l'extraction REST est séquentielle et tient en mémoire pandas, Spark n'apporterait rien ;
- le démarrage est beaucoup plus rapide (pas de session Spark à provisionner) ;
- la lecture/écriture de la table de tracking passe par **delta-rs** (`deltalake`), ce qui conditionne le protocole Delta de `load_tracking` (voir [03-load-tracking.md](03-load-tracking.md)).

> ⚠️ Ne pas convertir ce notebook en PySpark : cela casserait la compatibilité delta-rs de `load_tracking`.

## Paramètres (injectés par le pipeline)

| Paramètre | Requis | Description |
|---|---|---|
| `TABLE_NAME` | oui | Table M3 à extraire (ex. `CINACC`) |
| `CURSOR_COL` | oui | Colonne numérique croissante pour la pagination |
| `PK_COLS` | oui | Liste (ou string JSON) des colonnes de clé primaire, pour la déduplication |
| `LOADING_MODE` | oui | `full` ou `incremental` |
| `INCREMENTAL_COL` | si incremental | Colonne date `YYYYMMDD` pour la fenêtre incrémentale (ex. `EZLMDT` = last modified date) |
| `RUN_ID` | non | UUID du run pipeline (`@pipeline().RunId`) ; généré localement si absent |
| `SOURCE_NAME` | non | Défaut `M3` ; détermine le dossier `Files/<SOURCE_NAME>/<TABLE_NAME>` |

`PK_COLS` arrive du pipeline comme **string JSON** (`@string(item().pk_cols)`) et est désérialisé en liste au début du notebook.

## Résolution du contexte lakehouse

Les IDs du workspace et du lakehouse sont résolus **dynamiquement** depuis le contexte d'exécution (`notebookutils.runtime.context`), donc le notebook fonctionne dans n'importe quel workspace tant qu'un lakehouse par défaut est attaché :

```
LAKEHOUSE_BASE = abfss://<WORKSPACE_ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_ID>
```

Trois chemins en découlent :

- `Files/tenant.ionapi` — fichier de credentials ION (lu via `notebookutils.fs.head`) ;
- `Files/<SOURCE_NAME>/<TABLE_NAME>/load_date=<YYYY-MM-DD>/<run_id>_<date>.parquet` — sortie ;
- `Tables/dbo/load_tracking` — tracking, lu/écrit par delta-rs en chemin abfss complet.

### Authentification delta-rs vers OneLake

delta-rs ne sait pas s'authentifier seul sur OneLake (il tente IMDS `169.254.169.254`, qui timeout dans Fabric). On lui passe donc explicitement un token storage :

```python
{"bearer_token": notebookutils.credentials.getToken("storage"),
 "use_fabric_endpoint": "true"}
```

## Authentification à M3 (ION API)

Le fichier `Files/tenant.ionapi` (export standard Infor ION) fournit les champs `pu`/`ot` (URL token), `saak`/`sask` (service account), `ci`/`cs` (client id/secret), `iu`/`ti` (URL API + tenant). Le notebook fait un **OAuth2 grant_type=password** pour obtenir un bearer token, puis appelle :

```
GET {iu}/{ti}/M3/m3api-rest/v2/execute/EXPORTMI/Select
```

## Détection du mode effectif : `initial` vs `incremental`

Au démarrage, le notebook détermine le mode réellement appliqué (`mode_log`), qui peut différer du `LOADING_MODE` demandé :

```
last_run    = dernier run "loading" en Success pour cette table (lu dans load_tracking via delta-rs)
data_exists = le dossier Files/<SOURCE>/<TABLE> contient au moins une partition

mode_log = "initial"  si last_run est None OU data_exists est False
           sinon LOADING_MODE ("full" ou "incremental")
```

Le double critère (tracking **et** filesystem) protège contre les incohérences : si les fichiers ont été purgés mais que le tracking existe encore (ou l'inverse), on repart sur un chargement initial complet.

### Fenêtre incrémentale (high-water mark)

Le high-water mark est le **`DateID` du dernier run réussi** (date du jour du run, format `YYYYMMDD`), pas une valeur lue dans les données :

- **Mode `incremental`** : `WHERE incremental_col >= DateID_dernier_run AND incremental_col < DateID_aujourd'hui`.
  La borne inférieure est **inclusive** (un enregistrement modifié le jour du dernier run, après son passage, est rattrapé ; les doublons éventuels sont absorbés en silver). La borne supérieure est **exclusive** : les modifications du jour courant ne sont jamais prises, ce qui rend le run **déterministe** (relançable le même jour avec le même résultat).
- **Mode `initial` d'une table incrémentale** : `WHERE incremental_col < DateID_aujourd'hui` — même borne supérieure exclusive, pour que la première exécution incrémentale (qui repartira de ce `DateID`) ne chevauche pas le chargement initial.
- **Mode `full`** : aucune clause de fenêtre, la table est extraite entièrement à chaque run.

## Extraction paginée (EXPORTMI Select)

L'API EXPORTMI ne propose pas de pagination native ; elle est émulée par un **curseur applicatif** sur `CURSOR_COL` :

1. Requête `* from <TABLE> [where <fenêtre>] [and cursor_col >= <dernier_max>]` avec `maxrecs = 10000` (`BATCH_SIZE`).
2. Le premier batch demande les en-têtes (`HDRS=1`) ; les colonnes sont validées : `CURSOR_COL`, `INCREMENTAL_COL` (si nécessaire) et toutes les `PK_COLS` doivent exister, sinon échec immédiat.
3. Chaque enregistrement arrive comme une chaîne `REPL` séparée par `;`, splittée en liste.
4. **Déduplication par PK** : la condition `cursor_col >= dernier_max` étant inclusive (pour ne pas perdre de lignes partageant la valeur max du curseur à la frontière de deux batches), les lignes déjà vues sont écartées via un `set` de tuples PK.
5. Le max du curseur du batch devient le point de départ du batch suivant.
6. **Conditions d'arrêt** : batch vide, ou batch de taille < `BATCH_SIZE` (dernière page).

### Garde-fous

- **Pagination bloquée** : si le max du curseur n'avance pas d'un batch à l'autre, c'est que plus de `BATCH_SIZE` lignes partagent la même valeur de curseur → `RuntimeError` explicite (il faudrait un curseur secondaire). Mieux vaut échouer que boucler à l'infini ou perdre des données.
- **Curseur vide/non numérique** : les valeurs vides sont comptées et signalées ; si **aucune** valeur du batch n'est exploitable → `RuntimeError`.
- **Zéro ligne** : en mode `initial`/`full` c'est une erreur (la table ne peut pas être vide) ; en mode `incremental` c'est un résultat normal (aucune modification sur la fenêtre), le run se termine en Success sans rien écrire.

### Retry HTTP

Les appels EXPORTMI sont retentés jusqu'à **5 fois** sur les erreurs transitoires (statuts 408/429/500/502/503/504, timeouts et erreurs de connexion), avec un backoff exponentiel `30 s × 2^(n-1)` plafonné à 300 s. Le timeout client est de 180 s.

## Écriture parquet (zone `Files/`)

Si des lignes ont été extraites :

1. Construction d'un DataFrame pandas, **toutes colonnes en string** (l'auto-cast `auto_cast_columns` existe dans le notebook mais est volontairement désactivé — le typage est fait en silver).
2. Ajout des colonnes de lineage : `_load_date` (date locale Europe/Zurich), `_run_id`, `_ingested_at` (UTC).
3. Écriture locale dans `/tmp`, puis copie vers OneLake via `notebookutils.fs.cp`.

### Idempotence par partition

Avant la copie, la partition du jour `load_date=<date>` est **supprimée puis recréée** : relancer le run le même jour remplace le fichier du jour au lieu de l'empiler (1 seul fichier par date). Ce nettoyage est placé **dans** le bloc "il y a des lignes" : un re-run incrémental le même jour avec fenêtre vide **n'efface pas** la donnée déjà chargée le matin.

## Tracking

Quoi qu'il arrive (bloc `finally`), une ligne est ajoutée à `Tables/dbo/load_tracking` via **delta-rs** (`write_deltalake`, mode `append`, `schema_mode="merge"`), avec `Step = "loading"`, le statut (`🟢 Success` / `❌ Failed`), le message d'erreur éventuel, le `DateID`, la fenêtre, les compteurs (lignes, batches) et la durée. Les timestamps sont écrits en **ISO string** — voir [03-load-tracking.md](03-load-tracking.md) pour la contrainte de protocole Delta derrière ce choix.

En cas d'erreur, l'exception est **re-levée après** l'écriture du log : le notebook échoue (donc `Notebook2_transform` ne s'exécute pas), mais le run est tracé.
