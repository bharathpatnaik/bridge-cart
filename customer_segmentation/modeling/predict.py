import pandas as pd
from sklearn.base import BaseEstimator
from loguru import logger


def predict_segments(model: BaseEstimator, df: pd.DataFrame, feature_cols=None):
    """
    Use a trained clustering model (e.g. KMeans) to predict cluster labels
    for the given DataFrame.

    :param model: A fitted clustering model (e.g. KMeans).
    :param df: Pandas DataFrame containing the columns needed for prediction.
    :param feature_cols: List of column names to use for prediction. Defaults to ["age", "income", "clv"].
    :return: Pandas Series of predicted cluster labels.
    """
    if feature_cols is None:
        feature_cols = ["age", "income", "clv"]

    # Ensure columns exist
    for col in feature_cols:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame. Filling with 0.")
            df[col] = 0

    # Fill NA with 0 or an appropriate default
    predict_data = df[feature_cols].fillna(0)

    # Predict cluster
    labels = model.predict(predict_data)
    return pd.Series(labels, index=df.index)


def predict_rowwise(model: BaseEstimator, row: pd.Series, feature_cols=None):
    """
    If we plan to do row-by-row prediction.
    This is optional if we prefer applying 'model.predict' on the entire DataFrame at once.
    """
    if feature_cols is None:
        feature_cols = ["age", "income", "clv"]

    # Check for NaNs
    if row[feature_cols].isnull().any():
        return None

    return model.predict([row[feature_cols].values])[0]
