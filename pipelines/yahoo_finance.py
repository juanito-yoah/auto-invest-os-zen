import sqlite3
import yfinance as yf
from datetime import datetime

DB_PATH = "data/portfolio.db"

TICKERS = ["GOOGL", "NVDA", "PLTR", "CRWD", "DDOG", "DXCM", "HIMS", "V", "DUOL", "TSLA"]

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_metrics (
            ticker TEXT NOT NULL,
            company_name TEXT,
            current_price REAL,
            market_cap REAL,
            pe_ratio REAL,
            week_52_high REAL,
            week_52_low REAL,
            timestamp TEXT NOT NULL,
            PRIMARY KEY (ticker, timestamp)
        )
    """)
    conn.commit()

def fetch_metrics(ticker_symbol):
    try:
        info = yf.Ticker(ticker_symbol).info
        return {
            "ticker": ticker_symbol,
            "company_name": info.get("longName") or info.get("shortName"),
            "current_price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE") or info.get("forwardPE"),
            "week_52_high": info.get("fiftyTwoWeekHigh"),
            "week_52_low": info.get("fiftyTwoWeekLow"),
        }
    except Exception as e:
        print(f"  [WARN] Failed to fetch {ticker_symbol}: {e}")
        return None

def save_metrics(conn, metrics, timestamp):
    conn.execute("""
        INSERT OR REPLACE INTO watchlist_metrics
            (ticker, company_name, current_price, market_cap, pe_ratio, week_52_high, week_52_low, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metrics["ticker"],
        metrics["company_name"],
        metrics["current_price"],
        metrics["market_cap"],
        metrics["pe_ratio"],
        metrics["week_52_high"],
        metrics["week_52_low"],
        timestamp,
    ))
    conn.commit()

def fmt(value, prefix="", suffix="", decimals=2, scale=1):
    if value is None:
        return "N/A"
    return f"{prefix}{value / scale:,.{decimals}f}{suffix}"

def fmt_cap(value):
    if value is None:
        return "N/A"
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    return f"${value / 1e6:.2f}M"

def print_summary(results):
    col_widths = [6, 28, 10, 10, 8, 11, 10]
    headers = ["Ticker", "Company", "Price", "Mkt Cap", "P/E", "52W High", "52W Low"]

    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_row = "|" + "|".join(
        f" {h:<{w}} " for h, w in zip(headers, col_widths)
    ) + "|"

    print("\n" + sep)
    print(header_row)
    print(sep)

    for r in results:
        row = [
            r["ticker"],
            (r["company_name"] or "")[:col_widths[1]],
            fmt(r["current_price"], prefix="$"),
            fmt_cap(r["market_cap"]),
            fmt(r["pe_ratio"], decimals=1),
            fmt(r["week_52_high"], prefix="$"),
            fmt(r["week_52_low"], prefix="$"),
        ]
        print("|" + "|".join(f" {str(v):<{w}} " for v, w in zip(row, col_widths)) + "|")

    print(sep)
    print(f"\n{len(results)} tickers saved at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n")

def main():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    timestamp = datetime.utcnow().isoformat()
    results = []
    failed = []

    print(f"Fetching data for {len(TICKERS)} tickers...")
    for symbol in TICKERS:
        print(f"  {symbol}...", end=" ", flush=True)
        metrics = fetch_metrics(symbol)
        if metrics is None:
            failed.append(symbol)
            print("skipped")
            continue
        save_metrics(conn, metrics, timestamp)
        results.append(metrics)
        print("ok")

    conn.close()

    if results:
        print_summary(results)

    if failed:
        print(f"Skipped (no data): {', '.join(failed)}\n")

if __name__ == "__main__":
    main()
