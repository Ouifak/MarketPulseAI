# src/pipeline/silver/clean_trades.py

import sys
from datetime import datetime, timezone

import pandas as pd

from pipeline.bronze.utils import get_s3_client
from ingestion.config import S3_BUCKET_SILVER
from pipeline.silver.utils import normalize_timestamp, read_bronze, write_silver

DATASET = "trades"


def clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["symbol"] = df["symbol"].astype("string")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
    df["conditions"] = df["conditions"].apply(lambda x: tuple(x) if isinstance(x, list) else ())

    df = df.dropna(subset=["symbol", "price", "timestamp"])
    df = normalize_timestamp(df, "timestamp")
    df = df.drop_duplicates(subset=["symbol", "price", "volume", "timestamp", "conditions"])
    df = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    return df[["symbol", "price", "volume", "timestamp", "event_time", "conditions"]]


def main(date: str | None = None) -> None:
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = get_s3_client()

    raw = read_bronze(s3, DATASET, date)
    print(f"📥 Bronze {DATASET} {date} : {len(raw)} lignes brutes")

    clean_df = clean(raw)
    key = write_silver(s3, DATASET, date, clean_df)

    if key:
        print(f"✨ Silver écrit : s3://{S3_BUCKET_SILVER}/{key} ({len(clean_df)} lignes nettoyées)")
    else:
        print("⚠️  Rien à écrire (aucune donnée Bronze pour cette date)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)