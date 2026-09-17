# src/pipeline/gold/utils.py

import io

import pandas as pd

from ingestion.config import S3_BUCKET_SILVER, S3_BUCKET_GOLD


def read_silver(s3, dataset: str, date: str) -> pd.DataFrame:
    key = f"date={date}/{dataset}.parquet"
    try:
        body = s3.get_object(Bucket=S3_BUCKET_SILVER, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()
    return pd.read_parquet(io.BytesIO(body))


def write_gold(s3, dataset: str, date: str, df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    key = f"{dataset}/date={date}/{dataset}.parquet"
    s3.put_object(Bucket=S3_BUCKET_GOLD, Key=key, Body=buffer.getvalue())
    return key