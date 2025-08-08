"""
generate_tesla_csvs.py
Generates three CSV files for the Tesla pipeline:
 - tesla_stock_prices.csv
 - tesla_quarterly_financials.csv
 - tesla_production_sales.csv

Range: 2018-01-01 through 2025-08-08 (today)
Reproducible (seeded).
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ---------- CONFIG ----------
RNG_SEED = 42
np.random.seed(RNG_SEED)

START_DATE = pd.Timestamp("2018-01-01")
END_DATE = pd.Timestamp("2025-08-08")  # current project date
TICKER = "TSLA"

# Output filenames (these match the MySQL table names)
STOCK_CSV = "tesla_stock_prices.csv"
QUARTERLY_CSV = "tesla_quarterly_financials.csv"
PROD_CSV = "tesla_production_sales.csv"

# ---------- 1) STOCK PRICES (daily) ----------
def generate_stock_prices(start, end, init_price=20.0, drift=0.0007, vol=0.03):
    """
    Geometric Brownian Motion with daily seasonality & occasional jumps to mimic news.
    start, end: pandas.Timestamp
    init_price: starting close price (USD)
    drift: average daily drift
    vol: daily volatility
    """
    dates = pd.bdate_range(start=start, end=end)  # business days
    n = len(dates)
    # daily returns ~ N(drift, vol)
    rand = np.random.normal(loc=drift, scale=vol, size=n)
    # occasional jumps
    jumps = np.random.choice([0, 1, -1], size=n, p=[0.98, 0.01, 0.01]) * np.random.uniform(0.03, 0.25, size=n)
    returns = rand + jumps
    price = np.zeros(n)
    price[0] = init_price
    for i in range(1, n):
        price[i] = price[i-1] * np.exp(returns[i])
    # Build OHLCV with realistic intraday ranges
    open_p = price * (1 + np.random.normal(0, 0.002, n))
    close_p = price
    high_p = np.maximum(open_p, close_p) * (1 + np.abs(np.random.normal(0.002, 0.01, n)))
    low_p = np.minimum(open_p, close_p) * (1 - np.abs(np.random.normal(0.002, 0.01, n)))
    # Volume simulated with trend and noise
    base_vol = 30_000_00  # base in hundreds (adjusts for scale)
    vol_trend = np.linspace(0.8, 1.8, n)  # rising interest over years
    volume = (base_vol * vol_trend * (1 + np.random.normal(0, 0.3, n))).astype(int)
    df = pd.DataFrame({
        "trade_date": dates,
        "open_price": np.round(open_p, 2),
        "high_price": np.round(high_p, 2),
        "low_price": np.round(low_p, 2),
        "close_price": np.round(close_p, 2),
        "volume": volume
    })
    # Ensure no negative or zero prices
    for col in ["open_price", "high_price", "low_price", "close_price"]:
        df[col] = df[col].clip(lower=0.01)
    return df

# ---------- 2) QUARTERLY FINANCIALS ----------
def generate_quarterly_financials(start_q="2018Q1", end_date=END_DATE):
    # build quarters from 2018-Q1 to the quarter covering end_date
    quarters = pd.period_range(start=start_q, end=end_date.to_period("Q"), freq="Q")
    rows = []
    revenue_base = 2e9  # start ~$2B per quarter in 2018 (example)
    revenue_growth_annual = 0.45  # aggressive growth per year early; we model tapering
    for q in quarters:
        years_since_2018 = q.year - 2018 + (q.quarter - 1)/4
        # taper growth over time -> logistic-like slowdown
        growth_factor = (1 + revenue_growth_annual) ** (years_since_2018) / (1 + 0.02*years_since_2018)
        noise = np.random.normal(0, 0.08)
        revenue = revenue_base * growth_factor * (1 + noise)
        # net income margin gradual improvement with noise
        margin_base = 0.02 + 0.005 * (q.year - 2018)  # increases over years
        margin = np.clip(margin_base + np.random.normal(0, 0.03), -0.2, 0.4)
        net_income = revenue * margin
        # EPS: scale net income / outstanding shares proxy (use a pseudo number)
        shares = 1_000_000_000  # used only to derive EPS magnitude
        eps = (net_income / shares) * 1.0  # keep EPS in dollars
        rows.append({
            "quarter": f"Q{q.quarter}-{q.year}",
            "revenue": int(round(revenue)),
            "net_income": int(round(net_income)),
            "eps": round(float(eps), 2)
        })
    df = pd.DataFrame(rows)
    return df

# ---------- 3) PRODUCTION & DELIVERIES (quarterly) ----------
def generate_production_sales(q_fin_df):
    rows = []
    # baseline values (2018)
    model_sx_prod_base = 20000   # low numbers early
    model_sx_del_base = 19000
    model_3y_prod_base = 40000
    model_3y_del_base = 38000
    for idx, r in q_fin_df.iterrows():
        quarter = r["quarter"]
        # growth tied loosely to revenue growth
        revenue = r["revenue"]
        # scale factors to convert revenue to production units (toy model)
        total_scale = revenue / 1e9  # billions
        # production roughly proportional to scale with noise
        model_sx_prod = int(max(0, model_sx_prod_base * (1 + 0.12 * (total_scale - 2)) * (1 + np.random.normal(0, 0.12))))
        model_sx_del = int(max(0, model_sx_del_base * (1 + 0.12 * (total_scale - 2)) * (1 + np.random.normal(0, 0.12))))
        model_3y_prod = int(max(0, model_3y_prod_base * (1 + 0.35 * (total_scale - 2)) * (1 + np.random.normal(0, 0.15))))
        model_3y_del = int(max(0, model_3y_del_base * (1 + 0.35 * (total_scale - 2)) * (1 + np.random.normal(0, 0.15))))
        rows.append({
            "quarter": quarter,
            "model_s_x_production": model_sx_prod,
            "model_s_x_deliveries": model_sx_del,
            "model_3_y_production": model_3y_prod,
            "model_3_y_deliveries": model_3y_del
        })
    df = pd.DataFrame(rows)
    return df

# ---------- RUN GENERATION ----------
if __name__ == "__main__":
    # 1. Stock prices
    print("Generating stock prices...")
    stock_df = generate_stock_prices(START_DATE, END_DATE, init_price=22.0, drift=0.0009, vol=0.035)
    stock_df.to_csv(STOCK_CSV, index=False, date_format="%Y-%m-%d")
    print(f"Saved {STOCK_CSV} ({len(stock_df)} rows)")

    # 2. Quarterly financials
    print("Generating quarterly financials...")
    q_df = generate_quarterly_financials(start_q="2018Q1", end_date=END_DATE)
    q_df.to_csv(QUARTERLY_CSV, index=False)
    print(f"Saved {QUARTERLY_CSV} ({len(q_df)} rows)")

    # 3. Production & deliveries
    print("Generating production & deliveries...")
    prod_df = generate_production_sales(q_df)
    prod_df.to_csv(PROD_CSV, index=False)
    print(f"Saved {PROD_CSV} ({len(prod_df)} rows)")

    print("\nAll CSVs generated. Files:")
    print(f" - {STOCK_CSV}")
    print(f" - {QUARTERLY_CSV}")
    print(f" - {PROD_CSV}")
    print("\nNext: import these into MySQL (database: tesla_financials) or upload to your Azurite container.")
