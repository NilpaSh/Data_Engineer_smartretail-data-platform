"""
Statistical Analysis – SmartRetail Data Platform
==================================================
Author  : Nilpa
Created : 2026-05-03

Deep analytical layer using NumPy, SciPy, and Pandas.
Covers:
  - Descriptive statistics
  - Z-score & IQR outlier analysis
  - Correlation matrix
  - Revenue distribution analysis
  - Customer spend segmentation (KDE)
  - Hypothesis testing (t-test, chi-squared)
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

GOLD_DIR = Path("data/gold")


# ──────────────────────────────────────────────────────────────
# Descriptive Statistics
# ──────────────────────────────────────────────────────────────

def describe_revenue(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Compute comprehensive revenue statistics using NumPy.
    Returns a DataFrame suitable for a dashboard summary card.
    """
    rev = fact_orders["net_revenue"].dropna().to_numpy()

    q1, median, q3 = np.percentile(rev, [25, 50, 75])
    iqr = q3 - q1
    skewness = float(stats.skew(rev))
    kurt     = float(stats.kurtosis(rev))

    result = pd.DataFrame({
        "Metric": [
            "Count", "Sum", "Mean", "Median", "Std Dev",
            "Min", "Max", "Q1", "Q3", "IQR",
            "Skewness", "Kurtosis",
            "CV (Coeff of Variation)",
            "95th Percentile", "99th Percentile"
        ],
        "Value": [
            len(rev),
            np.round(np.sum(rev), 2),
            np.round(np.mean(rev), 2),
            np.round(median, 2),
            np.round(np.std(rev, ddof=1), 2),
            np.round(np.min(rev), 2),
            np.round(np.max(rev), 2),
            np.round(q1, 2),
            np.round(q3, 2),
            np.round(iqr, 2),
            np.round(skewness, 4),
            np.round(kurt, 4),
            np.round(np.std(rev, ddof=1) / np.mean(rev), 4),
            np.round(np.percentile(rev, 95), 2),
            np.round(np.percentile(rev, 99), 2),
        ]
    })
    return result


# ──────────────────────────────────────────────────────────────
# Outlier Analysis
# ──────────────────────────────────────────────────────────────

def detect_revenue_outliers(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Identify order-level revenue outliers using 3 methods:
      1. Z-score (> 3σ)
      2. Modified Z-score (MAD-based, > 3.5)
      3. IQR fence
    """
    df = fact_orders[["order_id", "net_revenue"]].copy()
    rev = df["net_revenue"].to_numpy(dtype=float)

    # ── Method 1: Z-score ──
    z_scores = np.abs(stats.zscore(rev, nan_policy="omit"))
    df["zscore"]        = np.round(z_scores, 3)
    df["is_zscore_out"] = z_scores > 3.0

    # ── Method 2: Modified Z-score (robust) ──
    median = np.nanmedian(rev)
    mad    = np.nanmedian(np.abs(rev - median))
    mod_z  = 0.6745 * (rev - median) / (mad if mad != 0 else 1)
    df["mod_zscore"]        = np.round(mod_z, 3)
    df["is_modz_out"]       = np.abs(mod_z) > 3.5

    # ── Method 3: IQR fence ──
    q1, q3 = np.percentile(rev, [25, 75])
    iqr    = q3 - q1
    lower, upper = q1 - 3.0 * iqr, q3 + 3.0 * iqr
    df["is_iqr_out"]    = (rev < lower) | (rev > upper)

    # ── Consensus: flagged by ≥ 2 methods ──
    df["outlier_votes"] = (
        df["is_zscore_out"].astype(int) +
        df["is_modz_out"].astype(int) +
        df["is_iqr_out"].astype(int)
    )
    df["is_outlier"] = df["outlier_votes"] >= 2

    logger.info(f"Outliers detected: {df['is_outlier'].sum():,} / {len(df):,} orders "
                f"({df['is_outlier'].mean()*100:.2f}%)")
    return df


# ──────────────────────────────────────────────────────────────
# Correlation Analysis
# ──────────────────────────────────────────────────────────────

def customer_correlation_matrix(
    customer_ltv: pd.DataFrame,
    numeric_cols: Optional[list] = None,
) -> pd.DataFrame:
    """
    Compute Pearson correlation matrix for customer numeric features.
    Uses NumPy for the computation.
    """
    if numeric_cols is None:
        numeric_cols = [
            "recency_days", "frequency", "monetary_value",
            "avg_order_value", "tenure_months", "predicted_ltv_24m",
            "rfm_score", "churn_risk_score"
        ]

    available = [c for c in numeric_cols if c in customer_ltv.columns]
    matrix    = customer_ltv[available].dropna()
    arr       = matrix.to_numpy(dtype=float)

    # NumPy correlation matrix
    corr_matrix = np.corrcoef(arr, rowvar=False)
    corr_df     = pd.DataFrame(corr_matrix, index=available, columns=available)

    logger.info(f"Correlation matrix computed: {corr_df.shape}")
    return corr_df.round(4)


# ──────────────────────────────────────────────────────────────
# Hypothesis Testing
# ──────────────────────────────────────────────────────────────

def test_email_vs_nonemail_revenue(
    fact_orders: pd.DataFrame,
    dim_customers: pd.DataFrame,
    alpha: float = 0.05,
) -> dict:
    """
    Two-sample independent t-test:
    H0: Mean order value of email subscribers = non-subscribers
    H1: Email subscribers spend more (one-tailed)
    """
    merged = fact_orders.merge(
        dim_customers[["customer_sk", "is_email_subscriber"]],
        on="customer_sk",
        how="left",
    )

    email_rev     = merged.loc[merged["is_email_subscriber"] == True,  "net_revenue"].dropna().to_numpy()
    nonemail_rev  = merged.loc[merged["is_email_subscriber"] == False, "net_revenue"].dropna().to_numpy()

    t_stat, p_val_2tailed = stats.ttest_ind(email_rev, nonemail_rev, equal_var=False)
    p_val_1tailed = p_val_2tailed / 2 if t_stat > 0 else 1.0

    effect_size = (np.mean(email_rev) - np.mean(nonemail_rev)) / np.sqrt(
        (np.std(email_rev, ddof=1)**2 + np.std(nonemail_rev, ddof=1)**2) / 2
    )

    result = {
        "test":                   "Welch t-test (one-tailed)",
        "hypothesis":             "Email subscribers spend more than non-subscribers",
        "n_email":                len(email_rev),
        "n_nonemail":             len(nonemail_rev),
        "mean_email":             np.round(np.mean(email_rev),    2),
        "mean_nonemail":          np.round(np.mean(nonemail_rev), 2),
        "t_statistic":            np.round(t_stat,          4),
        "p_value":                np.round(p_val_1tailed,   6),
        "cohens_d":               np.round(effect_size,     4),
        "reject_null":            bool(p_val_1tailed < alpha),
        "significance":           f"α = {alpha}",
        "interpretation": (
            "Email subscribers spend significantly more (reject H0)"
            if p_val_1tailed < alpha
            else "No significant difference (fail to reject H0)"
        ),
    }
    logger.info(f"t-test: t={t_stat:.3f}, p={p_val_1tailed:.4f}, reject_H0={result['reject_null']}")
    return result


def test_channel_revenue_anova(fact_orders: pd.DataFrame) -> dict:
    """
    One-way ANOVA: Does mean revenue differ across acquisition channels?
    """
    groups = [
        grp["net_revenue"].dropna().to_numpy()
        for _, grp in fact_orders.groupby("channel")
        if len(grp) > 30
    ]

    f_stat, p_val = stats.f_oneway(*groups)

    result = {
        "test":           "One-way ANOVA",
        "hypothesis":     "Revenue differs across channels",
        "n_groups":       len(groups),
        "f_statistic":    np.round(f_stat, 4),
        "p_value":        np.round(p_val,  6),
        "reject_null":    bool(p_val < 0.05),
        "interpretation": (
            "Significant revenue difference across channels (reject H0)"
            if p_val < 0.05
            else "No significant channel difference (fail to reject H0)"
        ),
    }
    return result


# ──────────────────────────────────────────────────────────────
# Revenue Distribution Analysis
# ──────────────────────────────────────────────────────────────

def revenue_distribution_bins(
    fact_orders: pd.DataFrame,
    n_bins: int = 20,
) -> pd.DataFrame:
    """
    Histogram binning of revenue distribution using NumPy.
    Used to build distribution charts in Power BI or matplotlib.
    """
    rev = fact_orders["net_revenue"].dropna().to_numpy(dtype=float)

    counts, edges = np.histogram(rev, bins=n_bins)
    bin_labels = [f"${edges[i]:.0f}–${edges[i+1]:.0f}" for i in range(len(edges)-1)]

    return pd.DataFrame({
        "bin_label":   bin_labels,
        "bin_low":     np.round(edges[:-1], 2),
        "bin_high":    np.round(edges[1:],  2),
        "count":       counts,
        "pct":         np.round(counts / counts.sum() * 100, 2),
    })


# ──────────────────────────────────────────────────────────────
# Seasonality Decomposition (NumPy FFT)
# ──────────────────────────────────────────────────────────────

def detect_seasonality(daily_sales: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Use NumPy FFT to detect dominant seasonal frequencies in daily revenue.
    Returns the top_n dominant periods (in days).
    """
    rev = daily_sales.sort_values("order_date")["net_revenue"].fillna(0).to_numpy(dtype=float)

    # Remove trend (simple detrend with polynomial fit)
    t = np.arange(len(rev))
    poly = np.polyfit(t, rev, deg=1)
    trend = np.polyval(poly, t)
    detrended = rev - trend

    # FFT
    fft_vals  = np.fft.rfft(detrended)
    freqs     = np.fft.rfftfreq(len(detrended), d=1)  # d=1 day
    power     = np.abs(fft_vals) ** 2

    # Skip DC component (freq=0)
    freqs  = freqs[1:]
    power  = power[1:]

    # Top dominant frequencies → periods
    top_idx     = np.argsort(power)[-top_n:][::-1]
    top_freqs   = freqs[top_idx]
    top_periods = np.where(top_freqs > 0, np.round(1 / top_freqs, 1), np.inf)

    return pd.DataFrame({
        "rank":        np.arange(1, top_n + 1),
        "period_days": top_periods,
        "frequency":   np.round(top_freqs, 6),
        "power":       np.round(power[top_idx], 2),
    })


# ──────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    fact   = pd.read_parquet(GOLD_DIR / "fact_orders.parquet")
    ltv    = pd.read_parquet(GOLD_DIR / "agg_customer_ltv.parquet")
    daily  = pd.read_parquet(GOLD_DIR / "agg_daily_sales.parquet")
    custs  = pd.read_parquet(Path("data/silver") / "customers.parquet")

    print("\n=== Revenue Descriptive Statistics ===")
    print(describe_revenue(fact).to_string(index=False))

    print("\n=== Outlier Detection ===")
    outliers = detect_revenue_outliers(fact)
    print(f"Outlier rows: {outliers['is_outlier'].sum():,}")

    print("\n=== Correlation Matrix ===")
    print(customer_correlation_matrix(ltv).to_string())

    print("\n=== Email Subscriber t-test ===")
    print(json.dumps(test_email_vs_nonemail_revenue(fact, custs), indent=2))

    print("\n=== Seasonality (FFT) ===")
    print(detect_seasonality(daily).to_string(index=False))
