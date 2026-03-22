import sqlite3
from datetime import datetime, timezone
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_PATH = "data/portfolio.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def latest_per_ticker(conn, ticker=None):
    """Return the most recent row(s) using the max timestamp per ticker."""
    if ticker:
        rows = conn.execute("""
            SELECT ticker, company_name, current_price, market_cap, pe_ratio,
                   week_52_high, week_52_low, timestamp
            FROM watchlist_metrics
            WHERE ticker = ?
              AND timestamp = (
                  SELECT MAX(timestamp) FROM watchlist_metrics WHERE ticker = ?
              )
        """, (ticker.upper(), ticker.upper())).fetchall()
    else:
        rows = conn.execute("""
            SELECT ticker, company_name, current_price, market_cap, pe_ratio,
                   week_52_high, week_52_low, timestamp
            FROM watchlist_metrics
            WHERE timestamp = (
                SELECT MAX(timestamp) FROM watchlist_metrics AS m2
                WHERE m2.ticker = watchlist_metrics.ticker
            )
            ORDER BY ticker ASC
        """).fetchall()
    return [dict(r) for r in rows]


@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/watchlist")
def watchlist():
    conn = get_db()
    try:
        rows = latest_per_ticker(conn)
    finally:
        conn.close()
    return jsonify(rows)


@app.get("/watchlist/<ticker>")
def watchlist_ticker(ticker):
    conn = get_db()
    try:
        rows = latest_per_ticker(conn, ticker)
    finally:
        conn.close()
    if not rows:
        return jsonify({"error": f"Ticker '{ticker.upper()}' not found"}), 404
    return jsonify(rows[0])


if __name__ == "__main__":
    port = 5000
    print(f"\n  auto-invest-os-zen API running at http://127.0.0.1:{port}\n")
    app.run(debug=True, port=port)
