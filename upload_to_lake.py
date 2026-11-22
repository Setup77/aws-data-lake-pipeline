"""Jour 2 : envoie un CSV dans la zone raw/ du Data Lake S3,
puis une version dédoublonnée en Parquet dans la zone clean/.

Usage :
    python upload_to_lake.py dim_companies.csv companies

Prérequis :
    pip install "boto3[crt]" polars
    aws login            (session active)
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from datetime import date
from pathlib import Path

import boto3
import polars as pl
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

BUCKET = os.getenv("LAKE_BUCKET", "de-lake-nokewi-547621305427-eu-north-1-an")
REGION = os.getenv("AWS_REGION", "eu-north-1")
INGEST_DATE = date.today().isoformat()


def upload_raw(s3, csv_path: Path, dataset: str) -> str:
    """Copie le fichier tel quel dans raw/, partitionné par date d'ingestion."""
    key = f"raw/{dataset}/ingest_date={INGEST_DATE}/{csv_path.name}"
    s3.upload_file(str(csv_path), BUCKET, key)
    return key


def upload_clean(s3, csv_path: Path, dataset: str) -> tuple[str, int, tuple[int, int]]:
    """Dédoublonne, convertit en Parquet et envoie dans clean/."""
    df = pl.read_csv(csv_path, infer_schema_length=10_000).unique(maintain_order=True)
    key = f"clean/{dataset}/ingest_date={INGEST_DATE}/{dataset}.parquet"

    with tempfile.TemporaryDirectory() as tmp:
        parquet_path = Path(tmp) / f"{dataset}.parquet"
        df.write_parquet(parquet_path)
        sizes = (csv_path.stat().st_size, parquet_path.stat().st_size)
        s3.upload_file(str(parquet_path), BUCKET, key)

    return key, df.height, sizes


def list_keys(s3, prefix: str):
    """Liste les objets d'un préfixe, page par page."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("Usage : python upload_to_lake.py <fichier.csv> <nom_du_dataset>")

    csv_path, dataset = Path(sys.argv[1]), sys.argv[2]
    if not csv_path.is_file():
        sys.exit(f"Fichier introuvable : {csv_path}")
    if not re.fullmatch(r"[a-z0-9_]+", dataset):
        sys.exit("Le nom du dataset ne peut contenir que des minuscules, chiffres et _")

    s3 = boto3.client("s3", region_name=REGION)

    try:
        raw_key = upload_raw(s3, csv_path, dataset)
        print(f"raw   : s3://{BUCKET}/{raw_key}")

        clean_key, rows, (csv_size, parquet_size) = upload_clean(s3, csv_path, dataset)
        print(f"clean : s3://{BUCKET}/{clean_key} ({rows} lignes)")
        print(f"Taille CSV : {csv_size} octets | Parquet : {parquet_size} octets")

        print(f"\nContenu de {dataset} dans le lake :")
        for prefix in (f"raw/{dataset}/", f"clean/{dataset}/"):
            for key, size in list_keys(s3, prefix):
                print(f"  {key}  ({size} octets)")
    except NoCredentialsError:
        sys.exit("Identifiants introuvables : relance `aws login` puis réessaie.")
    except ClientError as err:
        sys.exit(f"Erreur AWS : {err}")
    except BotoCoreError as err:
        sys.exit(f"Erreur de connexion : {err}")


if __name__ == "__main__":
    main()
