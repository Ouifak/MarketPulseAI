# src/pipeline/silver/utils.py

import io
import json
import pandas as pd

from ingestion.config import S3_BUCKET_BRONZE, S3_BUCKET_SILVER

FINNHUB_COLUMN_MAP = {
    "s": "symbol",
    "p": "price",
    "v": "volume",
    "t": "timestamp",
    "c": "conditions",
}


def normalize_timestamp(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    df["event_time"] = pd.to_datetime(df[column], unit="ms", utc=True)
    return df


def read_bronze(s3, dataset: str, date: str) -> pd.DataFrame:
    prefix = f"date={date}/"
    paginator = s3.get_paginator("list_objects_v2")
    rows: list[dict] = []

    for page in paginator.paginate(Bucket=S3_BUCKET_BRONZE, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=S3_BUCKET_BRONZE, Key=obj["Key"])["Body"].read()
            for line in body.decode("utf-8").splitlines():
                if line.strip():
                    rows.append(json.loads(line))

    if not rows:
        return pd.DataFrame(columns=["symbol", "price", "volume", "timestamp"])

    df = pd.DataFrame(rows)
    return df.rename(columns=FINNHUB_COLUMN_MAP)


def write_silver(s3, dataset: str, date: str, df: pd.DataFrame) -> str | None:
    if df.empty:
        return None

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    key = f"date={date}/{dataset}.parquet"
    s3.put_object(Bucket=S3_BUCKET_SILVER, Key=key, Body=buffer.getvalue())
    return key