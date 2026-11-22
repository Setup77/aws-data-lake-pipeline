"""Jour 5 : charge les données du Data Lake (S3) dans PostgreSQL (Docker), en local.

Lit :
    clean/companies/ingest_date=<dernière>/*.parquet   -> lake.companies_clean
    raw/countries/ingest_date=<dernière>/countries.json -> lake.countries
Chargement idempotent (ON CONFLICT DO UPDATE) : relancer ne crée aucun doublon.

Connexion PostgreSQL : variables d'environnement PGHOST, PGPORT, PGDATABASE,
PGUSER, PGPASSWORD (jamais de mot de passe dans le code).

Usage :
    python load_from_s3.py
"""

from __future__ import annotations

import io
import json
import os
import sys

import boto3
import polars as pl
import psycopg
from botocore.exceptions import BotoCoreError, ClientError

BUCKET = os.getenv("LAKE_BUCKET", "de-lake-nokewi-547621305427-eu-north-1-an")
REGION = os.getenv("AWS_REGION", "eu-north-1")

DDL = """
CREATE SCHEMA IF NOT EXISTS lake;

CREATE TABLE IF NOT EXISTS lake.companies_clean (
    nk_company_id   INTEGER PRIMARY KEY,
    company_key     INTEGER,
    company_name    TEXT,
    website_url     TEXT,
    email_frequency TEXT,
    is_unsubscribed BOOLEAN,
    created_at      TIMESTAMP,
    emails          TEXT,
    phones          TEXT,
    source_key      TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lake.countries (
    iso2_code    TEXT PRIMARY KEY,
    country_id   TEXT,
    name         TEXT,
    region       TEXT,
    income_level TEXT,
    capital_city TEXT,
    longitude    DOUBLE PRECISION,
    latitude     DOUBLE PRECISION,
    source_key   TEXT,
    loaded_at    TIMESTAMPTZ DEFAULT now()
);
"""

UPSERT_COMPANIES = """
INSERT INTO lake.companies_clean
    (nk_company_id, company_key, company_name, website_url, email_frequency,
     is_unsubscribed, created_at, emails, phones, source_key)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (nk_company_id) DO UPDATE SET
    company_key = EXCLUDED.company_key,
    company_name = EXCLUDED.company_name,
    website_url = EXCLUDED.website_url,
    email_frequency = EXCLUDED.email_frequency,
    is_unsubscribed = EXCLUDED.is_unsubscribed,
    created_at = EXCLUDED.created_at,
    emails = EXCLUDED.emails,
    phones = EXCLUDED.phones,
    source_key = EXCLUDED.source_key,
    loaded_at = now();
"""

UPSERT_COUNTRIES = """
INSERT INTO lake.countries
    (iso2_code, country_id, name, region, income_level, capital_city,
     longitude, latitude, source_key)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (iso2_code) DO UPDATE SET
    country_id = EXCLUDED.country_id,
    name = EXCLUDED.name,
    region = EXCLUDED.region,
    income_level = EXCLUDED.income_level,
    capital_city = EXCLUDED.capital_city,
    longitude = EXCLUDED.longitude,
    latitude = EXCLUDED.latitude,
    source_key = EXCLUDED.source_key,
    loaded_at = now();
"""


def latest_key(s3, prefix: str, suffix: str) -> str:
    """Clé la plus récente sous un préfixe (ingest_date= se trie comme une date)."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(suffix)
    ]
    if not keys:
        raise FileNotFoundError(f"Rien sous s3://{BUCKET}/{prefix} ({suffix})")
    return max(keys)


def read_bytes(s3, key: str) -> bytes:
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_companies(s3, cur) -> int:
    key = latest_key(s3, "clean/companies/", ".parquet")
    print(f"Lecture : s3://{BUCKET}/{key}")
    df = pl.read_parquet(io.BytesIO(read_bytes(s3, key)))
    rows = [
        (
            r["nk_company_id"], r["company_key"], r["company_name"], r["website_url"],
            r["email_frequency"], r["is_unsubscribed"], r["created_at"],
            r["emails"], r["phones"], key,
        )
        for r in df.iter_rows(named=True)
    ]
    cur.executemany(UPSERT_COMPANIES, rows)
    return len(rows)


def load_countries(s3, cur) -> int:
    key = latest_key(s3, "raw/countries/", ".json")
    print(f"Lecture : s3://{BUCKET}/{key}")
    payload = json.loads(read_bytes(s3, key))
    # API Banque mondiale : [métadonnées de pagination, liste de pays]
    countries = payload[1]
    rows = [
        (
            c.get("iso2Code"), c.get("id"), c.get("name"),
            (c.get("region") or {}).get("value"),
            (c.get("incomeLevel") or {}).get("value"),
            c.get("capitalCity") or None,
            to_float(c.get("longitude")), to_float(c.get("latitude")), key,
        )
        for c in countries
        if c.get("iso2Code")
    ]
    cur.executemany(UPSERT_COUNTRIES, rows)
    return len(rows)


def main() -> None:
    s3 = boto3.client("s3", region_name=REGION)
    try:
        # Connexion via les variables PG* ; transaction validée à la sortie du with
        with psycopg.connect() as conn, conn.cursor() as cur:
            cur.execute(DDL)
            n_comp = load_companies(s3, cur)
            n_ctry = load_countries(s3, cur)
            cur.execute("SELECT count(*) FROM lake.companies_clean")
            total_comp = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM lake.countries")
            total_ctry = cur.fetchone()[0]
        print(f"companies_clean : {n_comp} lignes traitées | total en base : {total_comp}")
        print(f"countries       : {n_ctry} lignes traitées | total en base : {total_ctry}")
    except (ClientError, BotoCoreError, FileNotFoundError, psycopg.Error) as err:
        sys.exit(f"Erreur : {err}")


if __name__ == "__main__":
    main()
