# Flux Gupta — Réutilisation de `files_to_delta` pour une autre source

La chaîne bronze n'est pas spécifique à M3 : l'étape **parquet → Delta** (`files_to_delta.ipynb`) est générique et sert aussi à la source **Gupta** (base PCF), configurée dans `table_gupta.json`.

## Configuration (`table_gupta.json`)

Mêmes clés que `table_m3.json`, avec les particularités suivantes :

- `source_name` : `gupta_pcf` (convention `gupta_<sousSource>`) ;
- `loading_mode` : `full` pour toutes les tables (ARTICLE, COMMANDEDETAIL, COMMANDEENTETE, ENTITEPERSONNE, CLIENT, …) ;
- `cursor_col` / `incremental_col` : vides — il n'y a pas d'extraction REST paginée côté Gupta ;
- `pk_cols` : vides (`["", ""]`) pour l'instant — voir l'impact sur le `hkey` ci-dessous.

## Différences avec le flux M3

### Extraction

L'étape de **loading n'utilise pas** `m3_loading_tables.ipynb` (spécifique à l'API EXPORTMI d'Infor M3). Les parquets Gupta sont déposés dans `Files/` par un autre mécanisme (activité Copy du pipeline / extraction dédiée), en respectant la **même convention de dépôt** que le loading M3 :

```
Files/gupta/<sousSource>/<TABLE>/load_date=YYYY-MM-DD/....parquet
```

C'est cette convention qui rend `files_to_delta` réutilisable tel quel.

### Résolution du dossier source dans `files_to_delta`

Le notebook détecte le préfixe `gupta_` et éclate le `source_name` en chemin à deux niveaux :

```python
if source_name.startswith("gupta_"):          # "gupta_pcf"
    SOURCE_DIR = f"Files/gupta/pcf/{table_name}"
else:                                          # "M3", "m3"
    SOURCE_DIR = f"Files/{source_name}/{table_name}"
```

La cible reste `Tables/dbo/tbl_m3_<TABLE>` (le préfixe `tbl_m3_` est aujourd'hui codé en dur dans le notebook, y compris pour Gupta — à renommer en préfixe neutre si cela devient gênant).

### `hkey` absent

`add_hkey` ignore les entrées de `pk_cols` vides ou blanches : avec la config actuelle (`["", ""]`), **aucune colonne `hkey` n'est ajoutée** aux tables Gupta (signalé par `[HKEY] PK_COLS vide` dans le log d'exécution). Dès que les clés primaires Gupta seront identifiées, il suffira de renseigner `pk_cols` dans `table_gupta.json` — aucun changement de code nécessaire ; le `mergeSchema` ajoutera la colonne `hkey` aux tables existantes au run suivant.

### Comportement d'écriture

Toutes les tables étant en `full`, chaque run charge la dernière partition `load_date=` en **snapshot historisé** (`replaceWhere` par `_load_date`), exactement comme les tables M3 full — voir [02-files-to-delta.md](02-files-to-delta.md). Le tracking dans `load_tracking` fonctionne à l'identique, avec `Source_name = gupta_pcf`.
