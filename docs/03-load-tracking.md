# `Tables/dbo/load_tracking` — Journal des exécutions

Table Delta unique, partagée par les deux notebooks, qui trace chaque exécution (une ligne par notebook et par table). Elle sert à la fois de **journal d'audit** et de **source du high-water mark** pour le mode incrémental de `m3_loading_tables` (lecture du `DateID` du dernier run `loading` réussi).

## Schéma

| Colonne | Type | Contenu |
|---|---|---|
| `Id` | string | UUID de la ligne de log |
| `Run_id` | string | UUID du run pipeline — **commun aux lignes loading et transform d'un même run** |
| `Source_name` | string | `M3`, `gupta_...` |
| `Step` | string | `loading` (m3_loading_tables) ou `transform` (files_to_delta) |
| `Table_name` | string | Table M3 (ex. `CINACC`) |
| `Mode` | string | loading : `initial` / `incremental` / `full` · transform : `full_snapshot` / `full_init` / `append` / `init` / `skip` |
| `Status` | string | `🟢 Success` ou `❌ Failed` |
| `Error_message` | string, nullable | Message d'exception si échec |
| `DateID` | int32 | Date du run `YYYYMMDD` — sert de high-water mark incrémental |
| `Start_time` / `End_time` | **string ISO 8601 (UTC)** | Volontairement string, pas timestamp — voir ci-dessous |
| `Duration` | string | `HH:MM:SS` |
| `Duration_seconds` | double | Durée en secondes |
| `Total_records` | int64 | Lignes extraites (loading) ou écrites (transform) |
| `Batches` | int32 | Nombre de batches EXPORTMI (loading) ; 0 en transform |
| `Cursor_col` / `Incremental_col` | string, nullable | Colonnes utilisées (loading) ; null en transform |
| `Window_start` / `Window_end` | int64, nullable | Fenêtre incrémentale `[start, end[` en `YYYYMMDD` ; null hors incremental |

Les deux notebooks écrivent **exactement le même schéma** (mêmes colonnes, même ordre, mêmes types). C'est une invariante à maintenir : toute évolution du schéma doit être appliquée des deux côtés.

## ⚠️ Contrainte critique : compatibilité delta-rs ↔ Spark

Deux moteurs Delta différents touchent cette table :

- **delta-rs** (`deltalake`, Python pur) : `m3_loading_tables` la **lit** (high-water mark) et y **écrit** sa ligne ;
- **Spark** : `files_to_delta` y **écrit** sa ligne.

Le piège : si Spark écrit une colonne de type **timestamp sans timezone** (`timestamp_ntz`), la table passe en protocole Delta v3 avec la reader feature `timestampNtz`, que **delta-rs ne sait pas lire** → `DeltaProtocolError` au run loading suivant, et toute la chaîne incrémentale est cassée.

### Règles à respecter

1. **Timestamps en string ISO** dans les deux notebooks (`start_time.isoformat()`), jamais en type timestamp/datetime. C'est ce qui maintient la table en protocole v1, lisible par les deux moteurs.
2. `files_to_delta` écrit via Spark avec un **schéma explicite** (`StructType`) — pas d'inférence qui pourrait introduire un type incompatible.
3. `m3_loading_tables` écrit via delta-rs avec des **dtypes pandas explicites** (`Int64` nullable pour les fenêtres, `string` pour les nullables).
4. delta-rs ne peut pas s'authentifier seul sur OneLake : passer `bearer_token` (`notebookutils.credentials.getToken("storage")`) + `use_fabric_endpoint: "true"` dans les `storage_options`.
5. `m3_loading_tables` doit rester **Python pur** et `files_to_delta` ne doit **pas** utiliser delta-rs (erreur de certificat en notebook Spark Fabric).

### Récupération si la table est corrompue

Si `load_tracking` a été écrite avec un type timestamp (protocole incompatible) et que delta-rs renvoie `DeltaProtocolError` :

```python
notebookutils.fs.rm("<abfss>/Tables/dbo/load_tracking", recurse=True)
```

puis relancer le pipeline : delta-rs/Spark recréeront la table avec le bon schéma. **Conséquence** : la perte du tracking fait repasser toutes les tables incrémentales en mode `initial` au run suivant (rechargement complet < date du jour), ce qui est sûr mais coûteux — à faire en connaissance de cause.

## Requêtes utiles

Dernier statut par table et par étape :

```sql
SELECT Table_name, Step, Mode, Status, End_time, Total_records, Duration
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY Table_name, Step ORDER BY End_time DESC) rn
  FROM dbo.load_tracking
)
WHERE rn = 1
ORDER BY Table_name, Step;
```

Corréler les deux étapes d'un même run :

```sql
SELECT Run_id, Table_name, Step, Mode, Status, Total_records, Duration
FROM dbo.load_tracking
WHERE Run_id = '<run_id_pipeline>'
ORDER BY Table_name, Step;
```
