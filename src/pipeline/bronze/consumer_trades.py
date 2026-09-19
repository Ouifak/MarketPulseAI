# src/consumers/consumer_trades.py

import json
import time
import uuid
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from confluent_kafka import Consumer

from ingestion.config import (
    BATCH_SIZE,
    BATCH_TIMEOUT_S,
    CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRADES,
    S3_ACCESS_KEY,
    S3_BUCKET_BRONZE,
    S3_ENDPOINT_URL,
    S3_SECRET_KEY,
)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # on commit nous-mêmes, après écriture S3 réussie
})
consumer.subscribe([KAFKA_TOPIC_TRADES])


def bronze_key() -> str:
    """bronze/date=YYYY-MM-DD/HHMMSS-uuid8.jsonl -- un fichier par batch, jamais d'append."""
    now = datetime.now(UTC)
    date_part = now.strftime("%Y-%m-%d")
    file_part = f"{now.strftime('%H%M%S')}-{uuid.uuid4().hex[:8]}.jsonl"
    return f"date={date_part}/{file_part}"


def flush(batch: list[dict], last_msg) -> None:
    if not batch:
        return
    body = "\n".join(json.dumps(r) for r in batch).encode("utf-8")
    key = bronze_key()
    s3.put_object(Bucket=S3_BUCKET_BRONZE, Key=key, Body=body, ContentType="application/x-ndjson")
    consumer.commit(message=last_msg)  # commit seulement après écriture réussie
    print(f"💾 {len(batch)} trades -> s3://{S3_BUCKET_BRONZE}/{key}")


def main() -> None:
    batch: list[dict] = []
    last_msg = None
    last_flush_time = time.time()

    print(f"🟤 Consumer Bronze démarré | topic={KAFKA_TOPIC_TRADES} -> bucket={S3_BUCKET_BRONZE}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    print(f"⚠️  Erreur Kafka: {msg.error()}")
                else:
                    trade = json.loads(msg.value().decode("utf-8"))
                    batch.append(trade)
                    last_msg = msg

                    if len(batch) >= BATCH_SIZE:
                        flush(batch, last_msg)
                        batch = []
                        last_flush_time = time.time()

            # condition indépendante du "if msg is not None" : le timeout doit
            # se déclencher même si aucun message n'est arrivé pendant ce poll
            if time.time() - last_flush_time >= BATCH_TIMEOUT_S and batch:
                flush(batch, last_msg)
                batch = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\n⏹️  Arrêt demandé")
    finally:
        flush(batch, last_msg)  # ne pas perdre le batch partiel en cours
        consumer.close()


if __name__ == "__main__":
    main()