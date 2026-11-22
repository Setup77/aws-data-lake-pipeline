"""Jour 4 : fonction Lambda d'ingestion API -> S3 raw/.

Variables d'environnement à définir dans la console Lambda :
    BUCKET   = de-lake-nokewi-547621305427-eu-north-1-an
    API_URL  = https://api.worldbank.org/v2/country?format=json&per_page=300

Deux modes selon l'événement reçu :
    - déclenchement planifié / test manuel : appelle l'API et écrit dans raw/countries/
    - événement S3 (dépôt dans raw/uploads/) : journalise le fichier reçu, n'écrit rien
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET"]
API_URL = os.environ["API_URL"]


def ingest() -> dict:
    request = urllib.request.Request(
        API_URL, headers={"User-Agent": "de-formation-lambda/1.0"}
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read()

    json.loads(payload)  # contrôle : refuse une réponse qui n'est pas du JSON valide

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/countries/ingest_date={today}/countries.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=payload, ContentType="application/json")

    logger.info("Ingestion OK : %s (%d octets)", key, len(payload))
    return {"status": "ok", "key": key, "bytes": len(payload)}


def lambda_handler(event, context):
    records = event.get("Records") or []
    if records and records[0].get("eventSource") == "aws:s3":
        # Le déclencheur écoute raw/uploads/ ; la fonction écrit dans raw/countries/.
        # Préfixes distincts = pas de boucle infinie.
        for record in records:
            logger.info(
                "Fichier reçu : s3://%s/%s",
                record["s3"]["bucket"]["name"],
                record["s3"]["object"]["key"],
            )
        return {"status": "s3-event-logged", "count": len(records)}

    return ingest()
