
import io

import pandas as pd

from ingestion.config import S3_BUCKET_GOLD


def read_features(s3, date: str) -> pd.DataFrame:
    key = f"features/date={date}/features.parquet"
    try:
        body = s3.get_object(Bucket=S3_BUCKET_GOLD, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()
    return pd.read_parquet(io.BytesIO(body))


def save_model(s3, date: str, model) -> str:
    """Sauvegarde le booster XGBoost au format JSON natif, dans gold/models/."""
    buffer = io.BytesIO()
    model.save_model(buffer)  # xgboost sait écrire directement dans un buffer binaire via son API bas niveau -> voir note plus bas
    buffer.seek(0)
    key = f"models/xgboost/date={date}/model.json"
    s3.put_object(Bucket=S3_BUCKET_GOLD, Key=key, Body=buffer.getvalue())
    return key