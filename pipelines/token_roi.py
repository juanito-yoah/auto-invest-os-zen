"""
Revenue per 1M Tokens — AI Research ROI metric
===============================================
Formula:
    revenue_per_1m_tokens = total_annual_revenue / (estimated_filing_tokens / 1_000_000)

Interpretation:
    "For every 1M tokens of a company's public disclosures processed by an AI,
     those disclosures represent $X of annual revenue."

    Higher value = more revenue-dense disclosures = higher ROI on AI research spend.

Token estimation model (annual public disclosures):
    Mega cap  (market cap > $500B) : 8M tokens  — large 10-K, multiple 10-Qs, dozens of 8-Ks,
                                                    quarterly earnings calls, proxy statements
    Large cap ($50B – $500B)       : 5M tokens
    Mid cap   ($5B  – $50B)        : 3M tokens
    Small cap (< $5B)              : 1.5M tokens

AI vendor pricing used (input tokens, per 1M):
    Claude Sonnet 4.6 : $3.00
    GPT-4o            : $2.50
    Gemini 1.5 Pro    : $1.25
"""

import sys
import yfinance as yf

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TICKERS = ["GOOGL", "NVDA", "PLTR", "CRWD", "DDOG", "DXCM", "HIMS", "V", "DUOL", "TSLA"]

# AI vendor pricing: (display name, $ per 1M input tokens)
VENDORS = [
    ("Claude Sonnet 4.6", 3.00),
    ("GPT-4o",            2.50),
    ("Gemini 1.5 Pro",    1.25),
]

def estimate_tokens(market_cap: float | None) -> tuple[int, str]:
    """Return (token_count, tier_label) based on market cap."""
    if market_cap is None:
        return 3_000_000, "mid (default)"
    if market_cap > 500e9:
        return 8_000_000, "mega cap"
    if market_cap > 50e9:
        return 5_000_000, "large cap"
    if market_cap > 5e9:
        return 3_000_000, "mid cap"
    return 1_500_000, "small cap"

def fmt_revenue(val):
    if val is None:
        return "N/A"
    if val >= 1e12:
        return f"${val/1e12:.2f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    return f"${val/1e6:.1f}M"

def fmt_tokens(n):
    return f"{n/1e6:.1f}M"

def fmt_cost(dollars):
    if dollars >= 1:
        return f"${dollars:.2f}"
    return f"${dollars*100:.1f}¢"

def fmt_roi(val):
    """Revenue per 1M tokens → friendly string."""
    if val is None:
        return "N/A"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.0f}M"
    return f"${val/1e3:.0f}K"

def fetch(symbol: str) -> dict | None:
    try:
        info = yf.Ticker(symbol).info
        return {
            "ticker":       symbol,
            "revenue":      info.get("totalRevenue"),
            "market_cap":   info.get("marketCap"),
            "company":      (info.get("longName") or info.get("shortName") or symbol)[:28],
        }
    except Exception as e:
        print(f"  [WARN] {symbol}: {e}")
        return None

def print_table(rows: list[dict]):
    # Column widths
    W = {
        "ticker":   6,
        "company":  28,
        "revenue":  10,
        "tokens":   8,
        "tier":     12,
    }
    vendor_w = 16

    # ── Header block ──────────────────────────────────────────────────────
    vendor_header = "".join(f"  {v[0]:<{vendor_w}}" for v in VENDORS)
    total_w = sum(W.values()) + len(W) * 3 + len(VENDORS) * (vendor_w + 2) + 2

    print()
    print("=" * total_w)
    print("  REVENUE PER 1M AI TOKENS  —  auto-invest-os-zen watchlist")
    print("=" * total_w)
    print(
        f"  {'Ticker':<{W['ticker']}}  "
        f"{'Company':<{W['company']}}  "
        f"{'Ann. Revenue':<{W['revenue']}}  "
        f"{'Est.Tokens':<{W['tokens']}}  "
        f"{'Cap Tier':<{W['tier']}}"
        f"{vendor_header}"
    )

    # Sub-header: vendor analysis cost row
    sub_cost = "".join(
        f"  {'Rev/1M tok | AI cost':<{vendor_w}}"
        for _ in VENDORS
    )
    print(
        f"  {'':<{W['ticker']}}  "
        f"{'':<{W['company']}}  "
        f"{'':<{W['revenue']}}  "
        f"{'':<{W['tokens']}}  "
        f"{'':<{W['tier']}}"
        f"{sub_cost}"
    )

    sep = "-" * total_w
    print(sep)

    # ── Data rows ─────────────────────────────────────────────────────────
    for r in rows:
        token_count, tier = estimate_tokens(r["market_cap"])
        token_m = token_count / 1_000_000  # tokens in millions

        rev = r["revenue"]
        roi_per_vendor = []
        for _, price_per_m in VENDORS:
            if rev is None:
                roi_per_vendor.append("N/A | N/A")
            else:
                roi = rev / token_m                         # revenue per 1M tokens
                cost = token_count / 1_000_000 * price_per_m  # $ to process all filings
                roi_per_vendor.append(f"{fmt_roi(roi):>9} | {fmt_cost(cost)}")

        vendor_cells = "".join(f"  {v:<{vendor_w}}" for v in roi_per_vendor)
        print(
            f"  {r['ticker']:<{W['ticker']}}  "
            f"{r['company']:<{W['company']}}  "
            f"{fmt_revenue(rev):<{W['revenue']}}  "
            f"{fmt_tokens(token_count):<{W['tokens']}}  "
            f"{tier:<{W['tier']}}"
            f"{vendor_cells}"
        )

    print(sep)

    # ── Methodology note ──────────────────────────────────────────────────
    print()
    print("  Methodology:")
    print("    Rev/1M tok  = Annual Revenue ÷ (estimated filing tokens / 1M)")
    print("    AI cost     = estimated filing tokens × vendor price per 1M tokens")
    print()
    print("  Token estimation (annual 10-K + 10-Qs + 8-Ks + earnings call transcripts):")
    print("    Mega cap  > $500B market cap  →  8M tokens")
    print("    Large cap $50B–$500B          →  5M tokens")
    print("    Mid cap   $5B–$50B            →  3M tokens")
    print("    Small cap < $5B               →  1.5M tokens")
    print()
    print("  AI vendor pricing (input tokens per 1M):")
    for name, price in VENDORS:
        print(f"    {name:<22}  ${price:.2f}")
    print()

def main():
    print(f"\nFetching revenue data for {len(TICKERS)} tickers...")
    rows = []
    for symbol in TICKERS:
        print(f"  {symbol}...", end=" ", flush=True)
        data = fetch(symbol)
        if data:
            rows.append(data)
            print(f"${data['revenue']/1e9:.1f}B" if data["revenue"] else "N/A")
        else:
            print("skipped")

    if rows:
        print_table(rows)

if __name__ == "__main__":
    main()
