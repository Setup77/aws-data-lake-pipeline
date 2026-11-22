"""Jour 3 : tourne sur EC2 avec le rôle ec2-etl-role (aucune clé d'accès).

Lit le dernier CSV de raw/companies/, nettoie et type les colonnes,
dédoublonne, puis écrit un Parquet dans clean/companies/.

Usage sur l'instance :
    python3 etl_to_s3.py
"""

from __future__ import annotations

import io
import os
import sys

import boto3
import polars as pl
from botocore.exceptions import BotoCoreError, ClientError

BUCKET = os.getenv("LAKE_BUCKET", "de-lake-nokewi-547621305427-eu-north-1-an")
REGION = os.getenv("AWS_REGION", "eu-north-1")
RAW_PREFIX = "raw/companies/"


def latest_raw_csv(s3) -> str:
    """Retourne la clé du CSV le plus récent (ingest_date= se trie comme une date)."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=RAW_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    if not keys:
        raise FileNotFoundError(f"Aucun CSV sous s3://{BUCKET}/{RAW_PREFIX}")
    return max(keys)


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Typage, nettoyage de texte, dédoublonnage sur la clé naturelle."""
    text_cols = ["company_name", "website_url", "email_frequency", "emails", "phones"]
    return (
        df.with_columns(
            pl.col("company_key").cast(pl.Int32, strict=False),
            pl.col("nk_company_id").cast(pl.Int32, strict=False),
            pl.col("is_unsubscribed")
            .str.to_lowercase()
            .is_in(["t", "true", "1"])
            .alias("is_unsubscribed"),
            pl.col("created_at").str.to_datetime(strict=False),
            *[pl.col(c).str.strip_chars() for c in text_cols],
        )
        .with_columns(pl.col("email_frequency").str.to_lowercase())
        .filter(pl.col("nk_company_id").is_not_null())
        .unique(subset=["nk_company_id"], keep="last", maintain_order=True)
    )


def main() -> None:
    s3 = boto3.client("s3", region_name=REGION)
    try:
        raw_key = latest_raw_csv(s3)
        print(f"Lecture : s3://{BUCKET}/{raw_key}")
        body = s3.get_object(Bucket=BUCKET, Key=raw_key)["Body"].read()

        # Tout en texte à la lecture : le typage est fait explicitement dans transform()
        raw = pl.read_csv(io.BytesIO(body), infer_schema_length=0)
        clean = transform(raw)
        print(f"Lignes lues : {raw.height} | lignes écrites : {clean.height}")

        ingest_part = raw_key.split("/")[2]  # ingest_date=AAAA-MM-JJ
        out_key = f"clean/companies/{ingest_part}/companies_ec2.parquet"
        buf = io.BytesIO()
        clean.write_parquet(buf)
        s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
        print(f"Écriture : s3://{BUCKET}/{out_key} ({buf.tell()} octets)")
    except (ClientError, BotoCoreError, FileNotFoundError) as err:
        sys.exit(f"Erreur : {err}")


if __name__ == "__main__":
    main()
