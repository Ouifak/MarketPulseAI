"""
Outils exposés aux agents LangGraph, interrogeant directement Gold via DuckDB.
Les agents n'accèdent jamais à Kafka/Bronze/Silver : uniquement Gold, déjà agrégé.
"""

import duckdb
from langchain_core.tools import tool

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET_GOLD,
    S3_ENDPOINT_URL,
    S3_SECRET_KEY,
)


def _connect() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB configurée pour lire les Parquet Gold sur MinIO via httpfs."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = S3_ENDPOINT_URL.replace("http://", "").replace("https://", "")
    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{S3_ACCESS_KEY}';
        SET s3_secret_access_key='{S3_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_region='us-east-1';
    """)
    return con


@tool
def get_stock_metrics(symbol: str, date: str) -> str:
    """Retourne les métriques agrégées (VWAP, volatilité, volume, rendement) pour un symbole et une date (format YYYY-MM-DD)."""
    con = _connect()
    query = f"""
        SELECT symbol, vwap, avg_price, total_volume, volatility, daily_return_pct, trade_count
        FROM read_parquet('s3://{S3_BUCKET_GOLD}/agg_stock_metrics/date={date}/agg_stock_metrics.parquet')
        WHERE symbol = '{symbol}'
    """
    result = con.execute(query).fetchdf()
    if result.empty:
        return f"Aucune donnée trouvée pour {symbol} le {date}."
    return result.to_string(index=False)


@tool
def detect_anomalies(date: str, volatility_threshold: float = 2.0) -> str:
    """
    Détecte les symboles avec une volatilité anormalement élevée (au-delà de
    `volatility_threshold` fois l'écart-type moyen tous symboles confondus) pour une date donnée.
    """
    con = _connect()
    query = f"""
        WITH stats AS (
            SELECT symbol, volatility,
                   AVG(volatility) OVER () AS avg_vol,
                   STDDEV(volatility) OVER () AS std_vol
            FROM read_parquet('s3://{S3_BUCKET_GOLD}/agg_stock_metrics/date={date}/agg_stock_metrics.parquet')
        )
        SELECT symbol, volatility
        FROM stats
        WHERE std_vol > 0 AND (volatility - avg_vol) / std_vol > {volatility_threshold}
        ORDER BY volatility DESC
    """
    result = con.execute(query).fetchdf()
    if result.empty:
        return "Aucune anomalie de volatilité détectée pour cette date."
    return result.to_string(index=False)


@tool
def get_5min_trend(symbol: str, date: str) -> str:
    """Retourne l'évolution du prix moyen par fenêtre de 5 minutes pour un symbole, sur une date."""
    con = _connect()
    query = f"""
        SELECT event_time, avg_price_5min, volume_5min, pct_change
        FROM read_parquet('s3://{S3_BUCKET_GOLD}/agg_5min/date={date}/agg_5min.parquet')
        WHERE symbol = '{symbol}'
        ORDER BY event_time
    """
    result = con.execute(query).fetchdf()
    if result.empty:
        return f"Aucune donnée 5min pour {symbol} le {date}."
    return result.to_string(index=False)