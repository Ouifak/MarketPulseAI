
"""
Entraînement XGBoost : prédire le rendement (price_return) du prochain trade
à partir des features Gold (moving_avg_10, price_return, rolling_vol_10).

Usage :
    python -m pipeline.forecast.train_xgboost [YYYY-MM-DD]
"""

import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

from pipeline.bronze.utils import get_s3_client
from forecasts.utils import read_features, save_model

FEATURE_COLS = ["moving_avg_10", "price_return", "rolling_vol_10", "volume"]
TARGET_COL = "target_next_return"


def build_supervised_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "event_time"]).copy()

    # La cible : le price_return du trade SUIVANT, pour le même symbole.
    # shift(-1) décale vers le "futur" -> à la ligne courante, on associe
    # ce qui se passera au trade d'après, exactement le principe supervisé
    # d'une prévision.
    df[TARGET_COL] = df.groupby("symbol", observed=True)["price_return"].shift(-1)

    # La dernière ligne de chaque symbole n'a pas de "trade suivant" -> pas de cible
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])
    return df


def time_based_split(df: pd.DataFrame, train_ratio: float = 0.8):
    """
    Découpage chronologique, PAS aléatoire : les premiers `train_ratio` %
    (dans le temps) servent à l'entraînement, le reste à l'évaluation.
    Un split aléatoire mélangerait passé et futur (fuite de données).
    """
    df = df.sort_values("event_time").reset_index(drop=True)
    cutoff = int(len(df) * train_ratio)
    return df.iloc[:cutoff], df.iloc[cutoff:]


def main(date: str | None = None) -> None:
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = get_s3_client()

    features = read_features(s3, date)
    print(f"📥 Gold features {date} : {len(features)} lignes")
    if features.empty:
        print("⚠️  Aucune feature disponible pour cette date")
        return

    dataset = build_supervised_dataset(features)
    print(f"🎯 Dataset supervisé : {len(dataset)} exemples (après création de la cible)")

    if len(dataset) < 20:
        print("⚠️  Trop peu de données pour un split train/test significatif")
        return

    train, test = time_based_split(dataset)
    print(f"✂️  Train : {len(train)} lignes | Test : {len(test)} lignes")

    model = XGBRegressor(
        n_estimators=100,
        max_depth=3,          # peu profond : on a très peu de données, un arbre
                               # profond mémoriserait le bruit plutôt qu'apprendre
        learning_rate=0.1,
        objective="reg:squarederror",
    )
    model.fit(train[FEATURE_COLS], train[TARGET_COL])

    predictions = model.predict(test[FEATURE_COLS])
    mae = mean_absolute_error(test[TARGET_COL], predictions)

    # Point de comparaison indispensable : un modèle "naïf" qui prédit
    # toujours 0% de rendement (aucun changement de prix). Si XGBoost ne
    # bat pas ce baseline, il n'apporte aucune valeur.
    baseline_mae = mean_absolute_error(test[TARGET_COL], np.zeros(len(test)))

    print(f"📊 MAE modèle XGBoost : {mae:.4f}")
    print(f"📊 MAE baseline (prédit toujours 0) : {baseline_mae:.4f}")

    key = save_model(s3, date, model.get_booster())
    print(f"💾 Modèle sauvegardé : s3://gold/{key}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)