"""
Revenue Forecasting – SmartRetail Data Platform
=================================================
Author  : Nilpa
Created : 2026-05-03

Implements multiple forecasting approaches:
  1. SARIMA (statsmodels)        – time-series baseline
  2. Prophet (Meta)              – handles seasonality & holidays
  3. Exponential Smoothing (NumPy manual) – lightweight fallback
  4. Linear regression trend     – for long-term projections

Outputs confidence intervals and model evaluation metrics.
"""

import logging
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
warnings.filterwarnings("ignore")

GOLD_DIR = Path("data/gold")


# ──────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────

def _mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Mean Absolute Percentage Error."""
    mask = actual != 0
    return float(np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100)


def _rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def _mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.abs(actual - predicted)))


def _prepare_daily_series(daily_sales: pd.DataFrame) -> pd.Series:
    """Return a clean daily revenue Series indexed by date."""
    ts = (
        daily_sales
        .assign(order_date=lambda df: pd.to_datetime(df["order_date"]))
        .sort_values("order_date")
        .set_index("order_date")["net_revenue"]
        .asfreq("D")
        .fillna(method="ffill")
    )
    return ts


# ──────────────────────────────────────────────────────────────
# 1. Triple Exponential Smoothing (Holt-Winters) – NumPy Manual
# ──────────────────────────────────────────────────────────────

class HoltWintersForecaster:
    """
    Manual implementation of Triple Exponential Smoothing (Holt-Winters)
    using NumPy.  Supports additive trend + additive seasonality.

    Parameters
    ----------
    alpha : float  – level smoothing (0 < α < 1)
    beta  : float  – trend smoothing (0 < β < 1)
    gamma : float  – seasonality smoothing (0 < γ < 1)
    period: int    – seasonal period (e.g., 7 for weekly)
    """

    def __init__(self, alpha: float = 0.3, beta: float = 0.1,
                 gamma: float = 0.2, period: int = 7):
        self.alpha  = alpha
        self.beta   = beta
        self.gamma  = gamma
        self.period = period
        self._level   = None
        self._trend   = None
        self._season  = None

    def fit(self, y: np.ndarray) -> "HoltWintersForecaster":
        n = len(y)
        m = self.period

        # Initialise level (average of first season)
        level = np.mean(y[:m])
        # Initialise trend (average of first season-over-season slopes)
        trend = (np.mean(y[m:2*m]) - np.mean(y[:m])) / m
        # Initialise seasonal indices
        season = y[:m] - level

        levels  = np.zeros(n)
        trends  = np.zeros(n)
        seasons = np.zeros(n + m)
        seasons[:m] = season

        for t in range(n):
            prev_level = level
            level  = self.alpha * (y[t] - seasons[t]) + (1 - self.alpha) * (prev_level + trend)
            trend  = self.beta  * (level - prev_level) + (1 - self.beta) * trend
            seasons[t + m] = self.gamma * (y[t] - level) + (1 - self.gamma) * seasons[t]
            levels[t]  = level
            trends[t]  = trend

        self._level  = level
        self._trend  = trend
        self._season = seasons[-m:]
        self._fitted_levels = levels
        self._n = n
        logger.info(f"HoltWinters fitted: level={level:.2f}, trend={trend:.4f}")
        return self

    def predict(self, h: int) -> np.ndarray:
        """Forecast h steps ahead."""
        preds = np.zeros(h)
        for i in range(h):
            seasonal_idx = i % self.period
            preds[i] = (self._level + (i + 1) * self._trend) + self._season[seasonal_idx]
        return np.maximum(preds, 0)   # revenue can't be negative

    def evaluate(self, y_test: np.ndarray) -> dict:
        """Evaluate on a test set (first refit, then predict len(y_test) steps)."""
        preds = self.predict(len(y_test))
        return {
            "model":  "HoltWinters",
            "mape":   round(_mape(y_test, preds), 3),
            "rmse":   round(_rmse(y_test, preds), 2),
            "mae":    round(_mae(y_test, preds),  2),
        }


# ──────────────────────────────────────────────────────────────
# 2. Linear Trend Projection (NumPy polyfit)
# ──────────────────────────────────────────────────────────────

class LinearTrendForecaster:
    """
    Fit a linear + optional polynomial trend to revenue
    using NumPy least-squares polynomial fitting.
    """

    def __init__(self, degree: int = 1):
        self.degree = degree
        self._coef  = None

    def fit(self, y: np.ndarray) -> "LinearTrendForecaster":
        t = np.arange(len(y), dtype=float)
        self._coef = np.polyfit(t, y, self.degree)
        logger.info(f"LinearTrend (degree={self.degree}) coefs: {self._coef.round(4)}")
        return self

    def predict(self, h: int, last_t: Optional[int] = None) -> np.ndarray:
        if last_t is None:
            raise ValueError("Pass last_t = len(train) - 1")
        future_t = np.arange(last_t + 1, last_t + 1 + h, dtype=float)
        return np.maximum(np.polyval(self._coef, future_t), 0)

    def evaluate(self, y_train: np.ndarray, y_test: np.ndarray) -> dict:
        preds = self.predict(len(y_test), last_t=len(y_train) - 1)
        return {
            "model": f"LinearTrend(deg={self.degree})",
            "mape":  round(_mape(y_test, preds), 3),
            "rmse":  round(_rmse(y_test, preds), 2),
            "mae":   round(_mae(y_test, preds),  2),
        }


# ──────────────────────────────────────────────────────────────
# 3. SARIMA via statsmodels
# ──────────────────────────────────────────────────────────────

class SARIMAForecaster:
    """
    Seasonal ARIMA wrapper using statsmodels.
    Default order chosen for weekly-seasonal daily data.
    """

    def __init__(self, order=(1,1,1), seasonal_order=(1,1,1,7)):
        self.order          = order
        self.seasonal_order = seasonal_order
        self._model_fit     = None

    def fit(self, ts: pd.Series) -> "SARIMAForecaster":
        try:
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            model = SARIMAX(
                ts,
                order=self.order,
                seasonal_order=self.seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            self._model_fit = model.fit(disp=False)
            logger.info(f"SARIMA fitted: AIC={self._model_fit.aic:.2f}")
        except ImportError:
            logger.warning("statsmodels not installed – SARIMA skipped")
        return self

    def predict(self, h: int) -> pd.DataFrame:
        if self._model_fit is None:
            return pd.DataFrame()
        forecast = self._model_fit.get_forecast(steps=h)
        ci = forecast.conf_int(alpha=0.10)   # 90% CI
        return pd.DataFrame({
            "forecast":  forecast.predicted_mean.values,
            "lower_90":  ci.iloc[:, 0].values,
            "upper_90":  ci.iloc[:, 1].values,
        })

    def evaluate(self, y_test: np.ndarray) -> dict:
        if self._model_fit is None:
            return {"model": "SARIMA", "status": "not_fitted"}
        preds_df = self.predict(len(y_test))
        preds    = preds_df["forecast"].to_numpy() if not preds_df.empty else np.zeros(len(y_test))
        return {
            "model": "SARIMA",
            "aic":   round(self._model_fit.aic, 2),
            "mape":  round(_mape(y_test, preds), 3),
            "rmse":  round(_rmse(y_test, preds), 2),
            "mae":   round(_mae(y_test, preds),  2),
        }


# ──────────────────────────────────────────────────────────────
# 4. Prophet via Meta's prophet library
# ──────────────────────────────────────────────────────────────

class ProphetForecaster:
    """
    Wrapper around Meta's Prophet for retail revenue forecasting.
    Adds US public holidays for improved accuracy.
    """

    def __init__(self, seasonality_mode: str = "multiplicative"):
        self.seasonality_mode = seasonality_mode
        self._model = None

    def fit(self, ts: pd.Series) -> "ProphetForecaster":
        try:
            from prophet import Prophet
            df = ts.reset_index().rename(columns={"order_date": "ds", "net_revenue": "y"})
            self._model = Prophet(
                seasonality_mode=self.seasonality_mode,
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
            )
            self._model.add_country_holidays(country_name="US")
            self._model.fit(df)
            logger.info("Prophet model fitted")
        except ImportError:
            logger.warning("prophet not installed – Prophet skipped")
        return self

    def predict(self, h: int) -> pd.DataFrame:
        if self._model is None:
            return pd.DataFrame()
        future = self._model.make_future_dataframe(periods=h)
        forecast = self._model.predict(future)
        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(h)

    def evaluate(self, ts_train: pd.Series, ts_test: pd.Series) -> dict:
        if self._model is None:
            return {"model": "Prophet", "status": "not_fitted"}
        forecast = self.predict(len(ts_test))
        preds    = np.maximum(forecast["yhat"].to_numpy(), 0)
        actual   = ts_test.to_numpy()
        return {
            "model": "Prophet",
            "mape":  round(_mape(actual, preds), 3),
            "rmse":  round(_rmse(actual, preds), 2),
            "mae":   round(_mae(actual, preds),  2),
        }


# ──────────────────────────────────────────────────────────────
# Ensemble Forecaster
# ──────────────────────────────────────────────────────────────

class EnsembleForecaster:
    """
    Weighted average of HoltWinters + LinearTrend forecasts.
    Weights are determined by inverse MAPE on validation set.
    """

    def __init__(self):
        self.hw   = HoltWintersForecaster(alpha=0.3, beta=0.1, gamma=0.2, period=7)
        self.lt   = LinearTrendForecaster(degree=2)
        self._weights = np.array([0.6, 0.4])

    def fit_evaluate(self, ts: pd.Series, test_size: int = 30) -> dict:
        train = ts.iloc[:-test_size].to_numpy()
        test  = ts.iloc[-test_size:].to_numpy()

        self.hw.fit(train)
        self.lt.fit(train)

        hw_eval = self.hw.evaluate(test)
        lt_eval = self.lt.evaluate(train, test)

        # Weight by inverse MAPE (lower error = higher weight)
        hw_err = hw_eval["mape"] + 1e-6
        lt_err = lt_eval["mape"] + 1e-6
        total  = 1/hw_err + 1/lt_err
        self._weights = np.array([1/hw_err / total, 1/lt_err / total])

        logger.info(f"Ensemble weights: HW={self._weights[0]:.3f}, LT={self._weights[1]:.3f}")
        return {
            "holt_winters": hw_eval,
            "linear_trend": lt_eval,
            "weights": {"holt_winters": round(self._weights[0], 3),
                        "linear_trend":  round(self._weights[1], 3)},
        }

    def predict(self, h: int, last_t: int) -> np.ndarray:
        hw_preds = self.hw.predict(h)
        lt_preds = self.lt.predict(h, last_t=last_t)
        return np.round(
            self._weights[0] * hw_preds + self._weights[1] * lt_preds, 2
        )


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    daily = pd.read_parquet(GOLD_DIR / "agg_daily_sales.parquet")
    ts    = _prepare_daily_series(daily)

    print(f"\nTime series: {ts.index[0].date()} → {ts.index[-1].date()} ({len(ts)} days)")

    # Train/test split (last 30 days = test)
    HORIZON = 30
    ts_train = ts.iloc[:-HORIZON]
    ts_test  = ts.iloc[-HORIZON:]

    # ── Ensemble ──
    ensemble = EnsembleForecaster()
    eval_results = ensemble.fit_evaluate(ts)
    print("\n=== Ensemble Model Evaluation ===")
    print(json.dumps(eval_results, indent=2))

    # ── 90-day forecast ──
    forecast_90d = ensemble.predict(h=90, last_t=len(ts_train) - 1)
    forecast_dates = pd.date_range(ts.index[-1] + pd.Timedelta("1D"), periods=90, freq="D")

    forecast_df = pd.DataFrame({
        "date":     forecast_dates,
        "forecast": forecast_90d,
    })
    print(f"\n=== 90-Day Revenue Forecast ===")
    print(forecast_df.head(15).to_string(index=False))
    print(f"\nForecasted Total (90d): ${forecast_df['forecast'].sum():,.2f}")

    # Save forecast
    out = Path("data/gold/forecast_90d.parquet")
    forecast_df.to_parquet(out, index=False)
    print(f"\nForecast saved → {out}")
