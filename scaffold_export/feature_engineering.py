"""Copied feature engineering helpers for scaffold export."""
from typing import List, Optional, Tuple
import pandas as pd


def generate_lags(df: pd.DataFrame, groupby: Optional[List[str]], target: str, lags: List[int]) -> pd.DataFrame:
    out = df.copy()
    if groupby:
        grp = out.groupby(groupby)
        for lag in lags:
            out[f"{target}_lag_{lag}"] = grp[target].shift(lag)
    else:
        for lag in lags:
            out[f"{target}_lag_{lag}"] = out[target].shift(lag)
    return out


def prepare_fold_features(train_df: pd.DataFrame, val_df: pd.DataFrame, features: List[str], target: str, lags: List[int]) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    combined = pd.concat([train_df, val_df])
    combined_with_lags = generate_lags(combined, None, target, lags)

    train_features = combined_with_lags.loc[train_df.index, features + [f"{target}_lag_{l}" for l in lags]].copy()
    val_features = combined_with_lags.loc[val_df.index, features + [f"{target}_lag_{l}" for l in lags]].copy()

    train_features = train_features.dropna()
    val_features = val_features.dropna()

    train_targets = train_df.loc[train_features.index, target]
    val_targets = val_df.loc[val_features.index, target]

    return train_features, train_targets, val_features, val_targets
