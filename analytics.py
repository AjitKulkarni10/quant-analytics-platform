import math
import numpy as np
import pandas as pd
from typing import Tuple

def ols_hedge_ratio(y, x):
    df = pd.concat([y, x], axis=1).dropna()
    if df.empty:
        return 1.0
    Y = df.iloc[:,0].values
    X = df.iloc[:,1].values
    if np.all(X == 0):
        return 0.0
    A = np.vstack([X, np.ones(len(X))]).T
    beta, intercept = np.linalg.lstsq(A, Y, rcond=None)[0]
    return float(beta)

def spread_and_zscore(y, x, beta, window = 50):
    df = pd.concat([y, x], axis=1).dropna()
    if df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    y_aligned = df.iloc[:,0]
    x_aligned = df.iloc[:,1]
    if beta is None:
        beta = ols_hedge_ratio(y_aligned, x_aligned)
    spread = y_aligned - beta * x_aligned
    rm = spread.rolling(window=window, min_periods=5).mean()
    rs = spread.rolling(window=window, min_periods=5).std()
    z = (spread - rm) / rs
    return spread, z

def rolling_correlation(s1, s2, window):
    df = pd.concat([s1, s2], axis=1).dropna()
    if df.empty:
        return pd.Series(dtype=float)
    return df.iloc[:,0].rolling(window, min_periods=5).corr(df.iloc[:,1])

def kalman_filter_beta(y, x):
    """
    Dynamic hedge ratio using a simple Kalman Filter.
    Returns beta_series (same length as y).
    """

    # convert to numpy arrays
    y = np.asarray(y)
    x = np.asarray(x)

    n = len(y)
    beta = np.zeros(n)

    # initial values
    beta_t = 0.0
    P = 1.0
    Q = 0.001  # process noise
    R = 1.0    # measurement noise

    for t in range(n):
        # Prediction step
        beta_pred = beta_t
        P = P + Q

        # Observation model
        H = x[t]

        # Avoid division by zero
        if H == 0:
            beta[t] = beta_pred
            continue

        # Measurement update
        K = P * H / (H * P * H + R)
        beta_t = beta_pred + K * (y[t] - H * beta_pred)
        P = (1 - K * H) * P

        beta[t] = beta_t

    return beta


def trading_signal(zscore, entry=2.0, exit=0.5):
    if zscore is None or math.isnan(zscore):
        return "NO_DATA"
    if zscore > entry:
        return "SELL_SPREAD → First coin expensive vs second → sell first, buy second"    # Short Y, Long X
    if zscore < -entry:
        return "BUY_SPREAD → First coin cheap vs second → buy first, sell second"     # Long Y, Short X

    if abs(zscore) < exit:
        return "EXIT"

    return "HODL"

