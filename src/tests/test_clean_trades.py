import pandas as pd

from pipeline.silver.clean_trades import clean


def test_clean_removes_true_duplicates():
    """Deux lignes strictement identiques doivent être fusionnées en une seule."""
    raw = pd.DataFrame({
        "symbol": ["AAPL", "AAPL"],
        "price": [190.0, 190.0],
        "volume": [100, 100],
        "timestamp": [1757761200123, 1757761200123],
        "conditions": [["1"], ["1"]],
    })
    result = clean(raw)
    assert len(result) == 1


def test_clean_keeps_same_timestamp_different_volume():
    """Deux trades au même instant mais volumes différents ne sont PAS des doublons."""
    raw = pd.DataFrame({
        "symbol": ["NVDA", "NVDA"],
        "price": [211.92, 211.92],
        "volume": [300, 100],
        "timestamp": [1789381987274, 1789381987274],
        "conditions": [["1", "8", "24"], ["1", "8", "24"]],
    })
    result = clean(raw)
    assert len(result) == 2