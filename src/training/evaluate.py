# src/training/evaluate.py
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """RMSE, MAE, R² for a single horizon."""
    return {
        "rmse": np.sqrt(mean_squared_error(y_true, y_pred)),
        "mae":  mean_absolute_error(y_true, y_pred),
        "r2":   r2_score(y_true, y_pred),
    }


def evaluate_all(
    y_true_by_horizon: dict[str, np.ndarray],
    y_pred_by_horizon: dict[str, np.ndarray],
) -> pd.DataFrame:
    """
    Compute metrics for each horizon and return a summary DataFrame.

    Args:
        y_true_by_horizon: {"target_1d": array, "target_2d": array, ...}
        y_pred_by_horizon: same keys, model predictions
    Returns:
        DataFrame with columns [horizon, rmse, mae, r2]
    """
    rows = []
    for key in y_true_by_horizon:
        metrics = compute_metrics(y_true_by_horizon[key], y_pred_by_horizon[key])
        rows.append({"horizon": key, **metrics})
    return pd.DataFrame(rows).sort_values("horizon").reset_index(drop=True)
