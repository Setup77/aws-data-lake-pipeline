# AWS Data Lake Pipeline

Projet de formation Data Engineer (semaines 9-11) : un Data Lake sur AWS, alimenté en
automatique, avec des accès réduits au strict nécessaire et un chargement final dans
PostgreSQL.

## Architecture

```
API Banque mondiale ─► Lambda ─► S3 raw/countries ──────────────┐
CSV ─► S3 raw/companies ─► EC2 (rôle IAM) ─► S3 clean/companies ─┤
                                                                 └─► load_from_s3.py ─► PostgreSQL (Docker)
```

Zones du lake : `raw/` (données brutes), `clean/` (Parquet nettoyé), `curated/` (réservé).
Les données sont partitionnées par date : `ingest_date=AAAA-MM-JJ/`.

## Contenu

| Fichier | Rôle |
|---|---|
| `upload_to_lake.py` | Dépose un CSV dans `raw/` et sa version Parquet dédoublonnée dans `clean/` |
| `lambda_function.py` | Fonction Lambda : appelle l'API publique et écrit dans `raw/countries/` ; journalise les dépôts dans `raw/uploads/` |
| `etl_to_s3.py` | Tourne sur EC2 avec un rôle IAM (aucune clé) : lit `raw/`, nettoie, écrit `clean/` |
| `load_from_s3.py` | Charge le lake dans PostgreSQL (schéma `lake`), de façon idempotente |

## Sécurité

- Aucune clé d'accès dans le code : rôles IAM (EC2, Lambda) et session temporaire en local (`aws login`).
- Moindre privilège : le rôle EC2 lit/écrit un seul bucket ; le rôle Lambda écrit seulement dans `raw/countries/`.
- Bucket : accès public bloqué, chiffrement SSE-S3, versioning, règle de cycle de vie.
- SSH limité à l'adresse IP de l'opérateur, jamais `0.0.0.0/0`.
- Mot de passe PostgreSQL lu dans des variables d'environnement (`PGPASSWORD`), jamais dans le code.

## Utilisation

Prérequis : Python 3.10+, AWS CLI connecté, Docker pour PostgreSQL.

```bash
pip install polars boto3 "psycopg[binary]"
```

Le nom du bucket se règle avec la variable `LAKE_BUCKET` (région `eu-north-1`).

```powershell
$env:LAKE_BUCKET = "mon-bucket"
python upload_to_lake.py dim_companies.csv companies

$env:PGHOST = "localhost"; $env:PGPORT = "5432"
$env:PGDATABASE = "analytics_db"; $env:PGUSER = "de_user"; $env:PGPASSWORD = "..."
python load_from_s3.py
```

## Coûts et nettoyage

Prévu pour un compte AWS gratuit : arrêter puis supprimer l'instance EC2, la paire de clés
et la règle EventBridge en fin de séance. S3 et Lambda restent quasi gratuits à cette échelle.
