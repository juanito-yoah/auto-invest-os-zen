"""
pipelines/kpi_audit.py — auto-invest-os-zen
============================================
Full KPI audit pipeline:
  Part 1 — 9 Standard Financial KPIs with confidence tiers
  Part 2 — AI Wallet Share (CORE + BIG_BET per-ticker segments)
  Part 3 — Public AI Commitment Tracker (seeded through early 2026)
  Part 4 — 4 output reports (console + text files + SQLite)
  Part 5 — GitHub Issues check/create (deduplicates against existing)

Data source: Yahoo Finance (yfinance) only.
Fields requiring SEC EDGAR / earnings transcripts / third-party TAM
are flagged PIPELINE PENDING — NOT estimated from yfinance.
"""

import sys
import sqlite3
import subprocess
import requests
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

# ── UTF-8 output on Windows ───────────────────────────────────────────────────
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / 'data' / 'portfolio.db'
REPORT_DIR = ROOT / 'data'
REPORT_DIR.mkdir(exist_ok=True)

# ── Core constants ────────────────────────────────────────────────────────────
TODAY        = date.today().isoformat()
TICKERS      = ['GOOGL','NVDA','PLTR','CRWD','DDOG','DXCM','HIMS','V','DUOL','TSLA']
SAAS_TICKERS = {'DDOG','CRWD','PLTR','DUOL'}
SUB_TICKERS  = {'DDOG','CRWD','PLTR','DUOL','DXCM'}
BURN_TICKERS = {'HIMS','DXCM','DUOL'}
AI_RATE      = 4.75    # $ per 1M tokens: 70% input@$2.50 + 30% output@$10.00
GITHUB_REPO  = 'juanito-yoah/auto-invest-os-zen'

REPORT_HEADER = (
    f"auto-invest-os-zen KPI Audit — {TODAY} — "
    "Conservative estimates only. All ASSUMED and PIPELINE PENDING metrics "
    "require manual review before any investment decision is made."
)

# Confidence tier labels
DIRECT       = 'DIRECT'
TRIANGULATED = 'TRIANGULATED'
ASSUMED      = 'ASSUMED'
PENDING      = 'PIPELINE PENDING'

# Status labels
S_OK   = 'OK'
S_WARN = 'WARN'
S_BAD  = 'BAD'
S_NA   = 'N/A'

# Display map for status
STATUS_ICON = {S_OK: '✅', S_WARN: '⚠️', S_BAD: '❌', S_NA: 'N/A'}
CONF_ICON   = {DIRECT: '✅ DIRECT', TRIANGULATED: '⚠️ TRIANGULATED',
               ASSUMED: '❌ ASSUMED', PENDING: 'PIPELINE PENDING'}

# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — AI WALLET SHARE SEGMENT DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
WALLET_SEGMENTS = {
    'GOOGL': {
        'CORE': {
            'segment_name': 'Advertising revenue vs Meta',
            'tam': 680_000_000_000, 'tam_source': 'IAB', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 264_600_000_000,
            'confidence': ASSUMED,
            'notes': 'Advertising revenue (Search+YouTube+Network) hardcoded from 2024 10-K. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
        'BIG_BET': {
            'segment_name': 'Google Cloud revenue vs Azure',
            'tam': 580_000_000_000, 'tam_source': 'IDC', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 43_228_000_000,
            'confidence': ASSUMED,
            'notes': 'Google Cloud FY2024 hardcoded from earnings release. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
    },
    'NVDA': {
        'CORE': {
            'segment_name': 'Data Center revenue vs AMD',
            'tam': 120_000_000_000, 'tam_source': 'IDC', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 115_163_000_000,
            'confidence': ASSUMED,
            'notes': 'Data Center segment FY2025 hardcoded from 10-K. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
        'BIG_BET': {
            'segment_name': 'AI Software/Services revenue vs Microsoft',
            'tam': 280_000_000_000, 'tam_source': 'Gartner', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — NVDA AI software/services revenue not separately disclosed. SEC EDGAR segment analysis needed.',
        },
    },
    'PLTR': {
        'CORE': {
            'segment_name': 'Government/Defense revenue vs Booz Allen',
            'tam': 45_000_000_000, 'tam_source': 'GOVWIN', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 1_144_000_000,
            'confidence': ASSUMED,
            'notes': 'Government segment FY2024 hardcoded from 10-K. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
        'BIG_BET': {
            'segment_name': 'Commercial Analytics revenue vs Snowflake',
            'tam': 95_000_000_000, 'tam_source': 'IDC', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 1_419_000_000,
            'confidence': ASSUMED,
            'notes': 'Commercial segment (US+International) FY2024 hardcoded from 10-K. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
    },
    'CRWD': {
        'CORE': {
            'segment_name': 'Endpoint Security revenue vs SentinelOne',
            'tam': 18_000_000_000, 'tam_source': 'Gartner', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total subscription revenue used as endpoint proxy — no module-level breakdown in yfinance. PIPELINE PENDING — SEC EDGAR ARR breakdown needed.',
        },
        'BIG_BET': {
            'segment_name': 'AI-native SOC Platform revenue vs Palo Alto',
            'tam': 42_000_000_000, 'tam_source': 'Gartner', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — AI-native SOC revenue not separately disclosed in yfinance.',
        },
    },
    'DDOG': {
        'CORE': {
            'segment_name': 'Cloud Monitoring revenue vs Dynatrace',
            'tam': 22_000_000_000, 'tam_source': 'Gartner', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total revenue used as cloud monitoring proxy. PIPELINE PENDING — product segment breakdown needed.',
        },
        'BIG_BET': {
            'segment_name': 'AI Observability revenue vs New Relic',
            'tam': 38_000_000_000, 'tam_source': 'Gartner', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — AI observability revenue not separately disclosed.',
        },
    },
    'HIMS': {
        'CORE': {
            'segment_name': 'Telehealth/GLP-1 revenue vs Teladoc',
            'tam': 50_000_000_000, 'tam_source': 'McKinsey', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total revenue used — telehealth/GLP-1 is core business. Segment split PIPELINE PENDING.',
        },
        'BIG_BET': {
            'segment_name': 'AI Personalized Treatment revenue vs Novo Nordisk',
            'tam': 180_000_000_000, 'tam_source': 'IQVIA', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — AI personalized treatment not separately disclosed.',
        },
    },
    'DXCM': {
        'CORE': {
            'segment_name': 'CGM Devices revenue vs Abbott',
            'tam': 18_000_000_000, 'tam_source': 'IQVIA', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total revenue used — CGM is essentially the entire business.',
        },
        'BIG_BET': {
            'segment_name': 'AI Diabetes Management Software vs Medtronic',
            'tam': 8_000_000_000, 'tam_source': 'Rock Health', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — AI diabetes management software not separately disclosed.',
        },
    },
    'DUOL': {
        'CORE': {
            'segment_name': 'Language Learning subscription revenue vs Babbel',
            'tam': 12_000_000_000, 'tam_source': 'HolonIQ', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total revenue used as subscription proxy. Sub/other split PIPELINE PENDING — SEC EDGAR needed.',
        },
        'BIG_BET': {
            'segment_name': 'AI-native Education Platform revenue vs Khan Academy',
            'tam': 180_000_000_000, 'tam_source': 'HolonIQ', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': TRIANGULATED,
            'notes': 'Total revenue used — Duolingo repositioned as AI-first platform 2024. All revenue treated as AI-native proxy.',
        },
    },
    'V': {
        'CORE': {
            'segment_name': 'Payment Processing network volume vs Mastercard',
            'tam': 3_200_000_000_000, 'tam_source': 'Nilson Report', 'tam_date': '2024',
            'method': 'yfinance_total', 'hardcoded_rev': None,
            'confidence': ASSUMED,
            'notes': 'TAM is card network VOLUME ($3.2T); company figure is REVENUE ($41.4B) — unit mismatch flagged. Wallet share % is revenue/volume ratio, not apples-to-apples. PIPELINE PENDING — Visa payment volume disclosure needed.',
        },
        'BIG_BET': {
            'segment_name': 'Enterprise AI Fraud/Risk Intelligence vs Palantir/CRWD',
            'tam': 18_000_000_000, 'tam_source': 'Nilson Report', 'tam_date': '2024',
            'method': 'pipeline_pending', 'hardcoded_rev': None,
            'confidence': PENDING,
            'notes': 'PIPELINE PENDING — Visa AI fraud revenue bundled in service fees, not separately disclosed.',
        },
    },
    'TSLA': {
        'CORE': {
            'segment_name': 'Automotive revenue vs BYD and Ford',
            'tam': 2_800_000_000_000, 'tam_source': 'OICA', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 77_119_000_000,
            'confidence': ASSUMED,
            'notes': 'Automotive segment FY2024 hardcoded from 10-K. PIPELINE PENDING — SEC EDGAR pull for automation.',
        },
        'BIG_BET': {
            'segment_name': 'Energy and Grid AI revenue vs NextEra',
            'tam': 320_000_000_000, 'tam_source': 'BloombergNEF', 'tam_date': '2024',
            'method': 'hardcoded', 'hardcoded_rev': 10_100_000_000,
            'confidence': ASSUMED,
            'notes': 'Energy Generation & Storage segment FY2024 hardcoded from 10-K. AI-specific attribution within energy PIPELINE PENDING.',
        },
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — AI COMMITMENT TRACKER SEED DATA (through early 2026)
# ─────────────────────────────────────────────────────────────────────────────
AI_COMMITMENT_SEEDS = {
    'GOOGL': [
        {'product_name': 'Gemini AI Models (1.5 Pro / 2.0)',
         'signal_type': 'ALL_THREE', 'signal_date': '2024-12-11',
         'source': 'Google I/O 2024; Q4 2024 earnings call Feb 2025 (Sundar Pichai)',
         'adoption_metric': 'Google Cloud AI revenue',
         'adoption_value': '$43.2B FY2024 (+35% YoY)',
         'status': 'DOUBLE_DOWN',
         'notes': 'S1: Gemini 2.0 product launch Dec 2024. S2: CEO cited AI across all products Q4 earnings. S3: Cloud AI revenue growth explicitly attributed. QoQ acceleration confirmed.'},
        {'product_name': 'Vertex AI Platform (Enterprise)',
         'signal_type': 'ALL_THREE', 'signal_date': '2024-05-14',
         'source': 'Google I/O 2024; Q3 2024 earnings call',
         'adoption_metric': 'Enterprise AI customers on Vertex',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'CONFIRMED',
         'notes': 'All three signals confirmed. Customer count metric not separately disclosed in yfinance.'},
    ],
    'NVDA': [
        {'product_name': 'Blackwell GPU Architecture (B200 / GB200)',
         'signal_type': 'ALL_THREE', 'signal_date': '2024-03-18',
         'source': 'GTC 2024 keynote; Q4 FY2025 earnings call Feb 2025 (Jensen Huang)',
         'adoption_metric': 'Data Center revenue',
         'adoption_value': '$115.2B FY2025 (+142% YoY)',
         'status': 'DOUBLE_DOWN',
         'notes': 'S1: Blackwell announced GTC 2024. S2: CEO Jensen Huang multiple earnings calls. S3: $115.2B Data Center FY2025. QoQ acceleration confirmed. NEW SEGMENT DISCLOSURE — Blackwell becoming dominant revenue driver.'},
        {'product_name': 'CUDA AI Platform / NIM Microservices',
         'signal_type': 'ALL_THREE', 'signal_date': '2024-03-18',
         'source': 'GTC 2024; multiple earnings calls 2024-2025',
         'adoption_metric': 'CUDA developer ecosystem + NIM adoption',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'CONFIRMED',
         'notes': 'All three signals confirmed. Developer count and NIM adoption metrics cited but not quantified separately in yfinance.'},
    ],
    'PLTR': [
        {'product_name': 'AIP (Artificial Intelligence Platform)',
         'signal_type': 'ALL_THREE', 'signal_date': '2023-04-26',
         'source': 'AIP launch press release Apr 2023; Q3 2024 earnings call (Alex Karp)',
         'adoption_metric': 'US Commercial revenue growth',
         'adoption_value': 'US Commercial +54% YoY Q3 2024. MANUAL UPDATE REQUIRED for latest quarter.',
         'status': 'DOUBLE_DOWN',
         'notes': 'S1: AIP product launch Apr 2023. S2: CEO Karp cited AIP every earnings call. S3: US Commercial growth explicitly attributed to AIP boot camp conversions. QoQ acceleration confirmed.'},
        {'product_name': 'Maven Smart System (Government AI)',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2024-01-15',
         'source': 'DoD press releases 2024; Q2 2024 earnings call',
         'adoption_metric': 'US Government revenue',
         'adoption_value': '$742M FY2024 (+16% YoY)',
         'status': 'WATCH',
         'notes': 'S1 & S2 confirmed. S3: Government revenue growing but AI attribution within contracts requires SEC EDGAR. MANUAL UPDATE REQUIRED.'},
    ],
    'CRWD': [
        {'product_name': 'Charlotte AI (Generative AI Security Assistant)',
         'signal_type': 'ALL_THREE', 'signal_date': '2023-05-24',
         'source': 'RSA Conference 2023 launch; Q3 FY2025 earnings call (George Kurtz)',
         'adoption_metric': 'Charlotte AI customer adoption %',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'CONFIRMED',
         'notes': 'S1: Charlotte AI launched RSA 2023. S2: CEO Kurtz cited Charlotte AI on multiple earnings calls. S3: Module adoption referenced on calls — specific percentage MANUAL UPDATE REQUIRED.'},
        {'product_name': 'Falcon Flex AI-Unified Platform',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2024-09-01',
         'source': "Fal.Con 2024 conference; Q3 FY2025 earnings call",
         'adoption_metric': 'Falcon Flex ARR contribution',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'WATCH',
         'notes': 'S1 & S2 confirmed at Fal.Con 2024. S3: ARR attributed to Flex not separately disclosed in yfinance.'},
    ],
    'DDOG': [
        {'product_name': 'Bits AI (Observability AI Copilot)',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2024-10-01',
         'source': 'DASH 2024 conference; Q3 2024 earnings call (Olivier Pomel)',
         'adoption_metric': 'Bits AI customer usage',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'WATCH',
         'notes': 'S1: Bits AI announced DASH 2024. S2: CEO Pomel referenced AI investment. S3: Product usage metrics not separately disclosed.'},
        {'product_name': 'LLM Observability Product',
         'signal_type': 'SIGNAL_1_ONLY', 'signal_date': '2024-06-01',
         'source': 'Product launch blog post Jun 2024',
         'adoption_metric': 'LLM monitoring customers',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'HYPE',
         'notes': 'S1 confirmed (product launched). Management earnings call commitment MANUAL UPDATE REQUIRED. No adoption metrics disclosed.'},
    ],
    'DXCM': [
        {'product_name': 'Stelo OTC CGM with AI Insights',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2024-08-01',
         'source': 'Stelo launch press release Aug 2024; Q3 2024 earnings call (Kevin Sayer)',
         'adoption_metric': 'Stelo user growth',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'WATCH',
         'notes': 'S1: Stelo OTC CGM launched Aug 2024. S2: CEO Sayer cited on Q3 2024 earnings. S3: Sales figures mentioned but AI attribution separate from device sales MANUAL UPDATE REQUIRED.'},
        {'product_name': 'DexCom ONE+ AI-Powered Glycemic Insights',
         'signal_type': 'SIGNAL_1_ONLY', 'signal_date': '2024-03-01',
         'source': 'Product announcement Mar 2024',
         'adoption_metric': 'N/A', 'adoption_value': 'N/A',
         'status': 'HYPE',
         'notes': 'S1 only — product announcement. Earnings call management mention MANUAL UPDATE REQUIRED.'},
    ],
    'HIMS': [
        {'product_name': 'AI Personalized Treatment Platform',
         'signal_type': 'SIGNAL_1_ONLY', 'signal_date': '2024-07-01',
         'source': 'Company press releases Jul 2024',
         'adoption_metric': 'N/A', 'adoption_value': 'N/A',
         'status': 'HYPE',
         'notes': 'S1: AI personalization announced in press releases. S2: CEO Andrew Dudum limited specific AI spend disclosure on earnings calls. S3: No adoption metrics. MANUAL UPDATE REQUIRED — check latest earnings call transcript.'},
        {'product_name': 'GLP-1 AI Dosing Optimization',
         'signal_type': 'SIGNAL_1_ONLY', 'signal_date': '2024-09-01',
         'source': 'Product roadmap disclosures Sep 2024',
         'adoption_metric': 'N/A', 'adoption_value': 'N/A',
         'status': 'HYPE',
         'notes': 'S1: GLP-1 AI dosing optimization mentioned in product materials. Earnings call and revenue attribution MANUAL UPDATE REQUIRED.'},
    ],
    'V': [
        {'product_name': 'Visa Protect (AI Fraud Detection)',
         'signal_type': 'ALL_THREE', 'signal_date': '2023-01-01',
         'source': 'Annual report 2023; FY2024 earnings call (Ryan McInerney)',
         'adoption_metric': 'Annual fraud prevented',
         'adoption_value': '$40B+ fraud prevented annually (per company disclosure)',
         'status': 'CONFIRMED',
         'notes': 'S1: Long-standing AI fraud product. S2: CEO McInerney referenced AI fraud prevention on FY2024 earnings. S3: $40B+ annual fraud prevention cited publicly.'},
        {'product_name': 'Visa Intelligent Commerce (AI Shopping Agents)',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2025-02-01',
         'source': 'Visa press release Feb 2025; Q2 FY2025 earnings call',
         'adoption_metric': 'Partner integrations',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'WATCH',
         'notes': 'S1: Visa Intelligent Commerce announced Feb 2025. S2: CEO confirmed on earnings call. S3: Revenue attribution not yet reported. NEW SEGMENT DISCLOSURE — potential double down signal. Manual review required.'},
    ],
    'DUOL': [
        {'product_name': 'Duolingo Max (AI-Powered Premium Subscription)',
         'signal_type': 'ALL_THREE', 'signal_date': '2023-03-14',
         'source': 'Duolingo Max launch press release Mar 2023; Q4 2024 earnings call (Luis von Ahn)',
         'adoption_metric': 'Paid subscribers / subscription revenue growth',
         'adoption_value': 'Paid subscribers 9.5M Q3 2024 (+55% YoY). MANUAL UPDATE REQUIRED for Max-specific share.',
         'status': 'DOUBLE_DOWN',
         'notes': 'S1: Duolingo Max launched Mar 2023 (GPT-4 Roleplay + Explain My Answer). S2: CEO von Ahn cited AI-first pivot, laid off contractors Nov 2023. S3: Subscriber growth attributed to AI features. QoQ acceleration confirmed.'},
        {'product_name': 'AI Video Call Practice Feature',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2024-09-01',
         'source': 'Product launch announcement Sep 2024; earnings call',
         'adoption_metric': 'Video call feature DAU',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call transcript',
         'status': 'WATCH',
         'notes': 'S1 & S2 confirmed. S3: Feature-level usage not separately disclosed.'},
    ],
    'TSLA': [
        {'product_name': 'Full Self-Driving (FSD) AI v12/v13',
         'signal_type': 'ALL_THREE', 'signal_date': '2024-10-10',
         'source': 'FSD v12/v13 release notes 2024; Q4 2024 earnings call (Elon Musk)',
         'adoption_metric': 'FSD cumulative miles / subscription count',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check latest earnings call for FSD subscription and attach rate.',
         'status': 'CONFIRMED',
         'notes': 'S1: FSD has multiple product releases (v12 late 2023, v13 2024). S2: Musk discusses FSD every earnings call. S3: FSD attach rate and subscription revenue partially disclosed.'},
        {'product_name': 'Dojo AI Supercomputer (Internal Training)',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2021-08-19',
         'source': 'Tesla AI Day 2021/2022; multiple earnings calls',
         'adoption_metric': 'External compute revenue',
         'adoption_value': 'N/A — no external revenue as of early 2026',
         'status': 'WATCH',
         'notes': 'S1: Dojo extensively announced. S2: Musk cited investment levels. S3: No external revenue from Dojo yet — internal use only.'},
        {'product_name': 'Optimus Robot (AI-Powered Humanoid)',
         'signal_type': 'SIGNALS_1_AND_2', 'signal_date': '2022-09-30',
         'source': 'Tesla AI Day 2 Sep 2022; Shareholder Meeting 2024; Q4 2024 earnings call',
         'adoption_metric': 'Units in internal use / production rate',
         'adoption_value': 'MANUAL UPDATE REQUIRED — check Q4 2024 earnings for Optimus production update.',
         'status': 'WATCH',
         'notes': 'S1: Optimus demo Sep 2022, updated Oct 2024. S2: Musk cited as most valuable asset. S3: Internal factory use reported but no external revenue yet.'},
    ],
}

# Status display strings
STATUS_DISPLAY = {
    'DOUBLE_DOWN': '✅✅ DOUBLE DOWN',
    'CONFIRMED':   '✅ CONFIRMED',
    'WATCH':       '⚠️ WATCH',
    'HYPE':        '🔴 HYPE',
    'FAILED':      '❌ FAILED',
}

# ─────────────────────────────────────────────────────────────────────────────
# PART 5 — GITHUB ISSUES TO ENSURE EXIST
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_ISSUES = [
    {'title': 'Expand KPI audit to full watchlist (30+ tickers + ETFs)',
     'body': 'Currently scoped to 10 tickers for MVP. Expand to full watchlist. Add separate ETF module tracking expense ratio, holdings concentration, YTD performance, sector exposure.'},
    {'title': 'Build SEC EDGAR pipeline for 10-K/10-Q data',
     'body': 'Yahoo Finance only covers surface financials. SEC EDGAR needed for NRR, segment revenue, MD&A AI commitment signals, R&D spend attribution. Build pipelines/sec_edgar.py.'},
    {'title': 'Add Gartner/IDC TAM data for AI Wallet Share denominators',
     'body': 'Establish quarterly update process for TAM figures. Consider hardcoded JSON config file with TAM source and date. Flag any TAM older than 12 months as stale.'},
    {'title': 'Add Perplexity MCP for earnings call transcript gap-filling',
     'body': 'Use to fill gaps where Yahoo Finance and SEC EDGAR return null, specifically NRR, AI commitment signals, management commentary on AI spend.'},
    {'title': 'Build Cowork Monday Morning Briefing automation',
     'body': 'Point Cowork at project folder, generate Monday_Briefing_[date].md automatically from portfolio.db.'},
    {'title': 'Add QoQ time-series tracking for AI Wallet Share',
     'body': 'Add QoQ delta calculation, flag >2pp swing as significant capital reallocation signal.'},
    {'title': 'Deploy Flask API to cloud for Loveable live data connection',
     'body': 'Deploy to Railway or Render free tier so Loveable dashboard can fetch live data and Demo Mode toggle shows real data.'},
]

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def create_tables():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS kpi_audit (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT NOT NULL,
            kpi_name         TEXT NOT NULL,
            value            TEXT,
            threshold        TEXT,
            status           TEXT,
            confidence_tier  TEXT,
            source_used      TEXT,
            notes            TEXT,
            timestamp        TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ai_wallet_share (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker                 TEXT NOT NULL,
            segment_type           TEXT NOT NULL,
            segment_name           TEXT,
            company_revenue        REAL,
            market_tam             REAL,
            wallet_share_pct       REAL,
            prior_wallet_share_pct REAL,
            qoq_change_pp          REAL,
            confidence_tier        TEXT,
            tam_source             TEXT,
            tam_date               TEXT,
            notes                  TEXT,
            timestamp              TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ai_commitment_tracker (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT NOT NULL,
            product_name     TEXT,
            signal_type      TEXT,
            signal_date      TEXT,
            source           TEXT,
            adoption_metric  TEXT,
            adoption_value   TEXT,
            status           TEXT,
            notes            TEXT,
            timestamp        TEXT NOT NULL
        );
        """)


def save_kpi(rows: list):
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO kpi_audit
              (ticker,kpi_name,value,threshold,status,confidence_tier,source_used,notes,timestamp)
            VALUES
              (:ticker,:kpi_name,:value,:threshold,:status,:confidence_tier,:source_used,:notes,:timestamp)
        """, rows)


def save_wallet(rows: list):
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO ai_wallet_share
              (ticker,segment_type,segment_name,company_revenue,market_tam,
               wallet_share_pct,prior_wallet_share_pct,qoq_change_pp,
               confidence_tier,tam_source,tam_date,notes,timestamp)
            VALUES
              (:ticker,:segment_type,:segment_name,:company_revenue,:market_tam,
               :wallet_share_pct,:prior_wallet_share_pct,:qoq_change_pp,
               :confidence_tier,:tam_source,:tam_date,:notes,:timestamp)
        """, rows)


def save_commitment(rows: list):
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO ai_commitment_tracker
              (ticker,product_name,signal_type,signal_date,source,
               adoption_metric,adoption_value,status,notes,timestamp)
            VALUES
              (:ticker,:product_name,:signal_type,:signal_date,:source,
               :adoption_metric,:adoption_value,:status,:notes,:timestamp)
        """, rows)


def get_prior_wallet_share(ticker: str, seg_type: str) -> Optional[float]:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT wallet_share_pct FROM ai_wallet_share
            WHERE ticker=? AND segment_type=? AND timestamp < ?
            ORDER BY timestamp DESC LIMIT 1
        """, (ticker, seg_type, TODAY)).fetchone()
    return row[0] if row else None


def get_prior_kpi9(ticker: str) -> Optional[float]:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT value FROM kpi_audit
            WHERE ticker=? AND kpi_name='Revenue_per_1M_Tokens' AND timestamp < ?
            ORDER BY timestamp DESC LIMIT 1
        """, (ticker, TODAY)).fetchone()
    if not row or not row[0]:
        return None
    try:
        raw = str(row[0]).replace('$','').strip()
        if raw.endswith('T'): return float(raw[:-1]) * 1e12
        if raw.endswith('B'): return float(raw[:-1]) * 1e9
        if raw.endswith('M'): return float(raw[:-1]) * 1e6
        if raw.endswith('K'): return float(raw[:-1]) * 1e3
        return float(raw)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# YFINANCE DATA FETCHER
# ─────────────────────────────────────────────────────────────────────────────
def fetch_data(symbol: str) -> dict:
    print(f"    {symbol}...", end=' ', flush=True)
    t = yf.Ticker(symbol)
    try:
        info = t.info
    except Exception:
        info = {}

    def safe_df(attr):
        try:
            df = getattr(t, attr)
            return df if (df is not None and not df.empty) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    inc = safe_df('income_stmt')
    cf  = safe_df('cashflow')
    bs  = safe_df('balance_sheet')
    print(f"ok  [{(info.get('longName') or info.get('shortName') or '?')[:32]}]")
    return {'symbol': symbol, 'info': info, 'income': inc, 'cashflow': cf, 'balance': bs}


def sv(df: pd.DataFrame, *keys, col: int = 0) -> Optional[float]:
    """Safe DataFrame value lookup — try multiple row keys, return float or None."""
    if df is None or df.empty:
        return None
    for k in keys:
        if k in df.index:
            try:
                series = df.loc[k]
                v = series.iloc[col] if hasattr(series, 'iloc') else series
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    return float(v)
            except Exception:
                pass
    return None


def info_val(info: dict, *keys) -> Optional[float]:
    for k in keys:
        v = info.get(k)
        if v is not None:
            try:
                f = float(v)
                return None if pd.isna(f) else f
            except Exception:
                pass
    return None


def fmt_pct(v) -> str:
    return f"{v:.2f}%" if v is not None else 'N/A'


def fmt_money(v) -> str:
    if v is None: return 'N/A'
    if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:  return f"${v/1e6:.2f}M"
    return f"${v:.0f}"


def kpi_row(ticker, name, value, threshold, status, confidence, source, notes) -> dict:
    return {
        'ticker': ticker, 'kpi_name': name, 'value': str(value),
        'threshold': threshold, 'status': status, 'confidence_tier': confidence,
        'source_used': source, 'notes': notes, 'timestamp': TODAY,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — KPI COMPUTERS
# ─────────────────────────────────────────────────────────────────────────────

def kpi_return_on_capital(d: dict) -> dict:
    t   = d['symbol']
    inc = d['income']
    bs  = d['balance']

    ebit  = sv(inc, 'EBIT')
    proxy = False
    if ebit is None:
        ebit  = sv(inc, 'Operating Income')
        proxy = ebit is not None

    net_ppe  = sv(bs, 'Net PPE')
    work_cap = sv(bs, 'Working Capital')
    if work_cap is None:
        ca = sv(bs, 'Current Assets')
        cl = sv(bs, 'Current Liabilities')
        work_cap = (ca - cl) if (ca is not None and cl is not None) else None

    if ebit is None or net_ppe is None or work_cap is None:
        return kpi_row(t, 'Return_on_Capital', 'N/A',
            '>20% OK | 10-20% WARN | <10% BAD', S_NA, PENDING,
            'yfinance income_stmt / balance_sheet',
            f'PIPELINE PENDING — missing fields: ebit={ebit is not None}, net_ppe={net_ppe is not None}, working_cap={work_cap is not None}')

    denom = net_ppe + work_cap
    if denom == 0:
        return kpi_row(t, 'Return_on_Capital', 'N/A', '>20% OK | 10-20% WARN | <10% BAD',
            S_NA, TRIANGULATED, 'yfinance', 'Denominator (Net PPE + Working Capital) is zero.')

    roc    = ebit / denom * 100
    status = S_OK if roc > 20 else (S_WARN if roc >= 10 else S_BAD)
    conf   = TRIANGULATED if proxy else DIRECT
    note   = (('operatingIncome used as EBIT proxy — marked TRIANGULATED. ' if proxy else '') +
              f'EBIT={fmt_money(ebit)}, NetPPE={fmt_money(net_ppe)}, WorkCap={fmt_money(work_cap)}')
    return kpi_row(t, 'Return_on_Capital', fmt_pct(roc),
        '>20% OK | 10-20% WARN | <10% BAD', status, conf,
        'EBIT|OperatingIncome + Net PPE + Working Capital (yfinance income_stmt/balance_sheet)', note)


def kpi_fcf_margin(d: dict) -> dict:
    t    = d['symbol']
    info = d['info']
    cf   = d['cashflow']
    inc  = d['income']

    ocf   = sv(cf, 'Operating Cash Flow') or info_val(info, 'operatingCashflow')
    capex = sv(cf, 'Capital Expenditure')
    rev   = sv(inc, 'Total Revenue', 'Operating Revenue') or info_val(info, 'totalRevenue')

    if ocf is None or rev is None or rev == 0:
        return kpi_row(t, 'FCF_Margin', 'N/A',
            'SaaS: >25% OK | 15-25% WARN | <15% BAD | General: >15% OK | 5-15% WARN | <5% BAD',
            S_NA, PENDING, 'yfinance cashflow/income_stmt',
            f'PIPELINE PENDING — ocf={ocf is not None}, rev={rev is not None}')

    capex_abs = abs(capex) if capex is not None else 0
    fcf       = ocf - capex_abs
    margin    = fcf / rev * 100

    is_saas = t in SAAS_TICKERS
    if is_saas:
        threshold = 'SaaS: >25% OK | 15-25% WARN | <15% BAD'
        status    = S_OK if margin > 25 else (S_WARN if margin >= 15 else S_BAD)
    else:
        threshold = 'General: >15% OK | 5-15% WARN | <5% BAD'
        status    = S_OK if margin > 15 else (S_WARN if margin >= 5 else S_BAD)

    conf = DIRECT if capex is not None else TRIANGULATED
    note = (f'OCF={fmt_money(ocf)}, CAPEX={fmt_money(capex_abs)}, Rev={fmt_money(rev)}. '
            + ('SaaS threshold applied.' if is_saas else 'General threshold applied.')
            + ('' if capex is not None else ' CAPEX unavailable — OCF used as FCF proxy, marked TRIANGULATED.'))
    return kpi_row(t, 'FCF_Margin', fmt_pct(margin), threshold, status, conf,
        'Operating Cash Flow + Capital Expenditure + Total Revenue (yfinance cashflow/income_stmt)', note)


def kpi_gross_margin(d: dict) -> dict:
    t    = d['symbol']
    inc  = d['income']
    info = d['info']

    gp  = sv(inc, 'Gross Profit') or info_val(info, 'grossProfits')
    rev = sv(inc, 'Total Revenue', 'Operating Revenue') or info_val(info, 'totalRevenue')

    if gp is None or rev is None or rev == 0:
        return kpi_row(t, 'Gross_Profit_Margin', 'N/A',
            '>70% premium OK | 50-70% strong OK | 25-50% WARN | <25% BAD',
            S_NA, PENDING, 'yfinance income_stmt',
            f'PIPELINE PENDING — gp={gp is not None}, rev={rev is not None}')

    gpm = gp / rev * 100
    if gpm > 70:   status, tier = S_OK, 'premium'
    elif gpm > 50: status, tier = S_OK, 'strong'
    elif gpm > 25: status, tier = S_WARN, 'moderate'
    else:          status, tier = S_BAD, 'low'

    return kpi_row(t, 'Gross_Profit_Margin', fmt_pct(gpm),
        '>70% premium OK | 50-70% strong OK | 25-50% WARN | <25% BAD',
        status, DIRECT, 'Gross Profit + Total Revenue (yfinance income_stmt)',
        f'GrossProfit={fmt_money(gp)}, Revenue={fmt_money(rev)}, Tier={tier}')


def kpi_earnings_yield(d: dict) -> dict:
    t    = d['symbol']
    inc  = d['income']
    info = d['info']

    ebit = sv(inc, 'EBIT', 'Operating Income')
    ev   = info_val(info, 'enterpriseValue')

    if ebit is None or ev is None or ev == 0:
        return kpi_row(t, 'Earnings_Yield', 'N/A',
            '>5% OK | 2-5% WARN | <2% BAD', S_NA, PENDING,
            'yfinance income_stmt/info',
            f'PIPELINE PENDING — ebit={ebit is not None}, ev={ev is not None}')

    ey     = ebit / ev * 100
    status = S_OK if ey > 5 else (S_WARN if ey >= 2 else S_BAD)
    return kpi_row(t, 'Earnings_Yield', fmt_pct(ey),
        '>5% OK | 2-5% WARN | <2% BAD', status, DIRECT,
        'EBIT + Enterprise Value (yfinance income_stmt/info)',
        f'EBIT={fmt_money(ebit)}, EV={fmt_money(ev)}')


def kpi_peg(d: dict) -> dict:
    t    = d['symbol']
    info = d['info']

    pe       = info_val(info, 'trailingPE')
    pe_proxy = False
    if pe is None:
        pe       = info_val(info, 'forwardPE')
        pe_proxy = pe is not None

    eg = info_val(info, 'earningsGrowth')

    if pe is None:
        return kpi_row(t, 'PEG_Ratio', 'N/A',
            '<0.5 exceptional OK | 0.5-1.0 strong OK | 1-2 WARN | >2 BAD',
            S_NA, PENDING, 'yfinance info trailingPE/forwardPE',
            'PIPELINE PENDING — no P/E ratio available in yfinance.')

    if eg is None:
        return kpi_row(t, 'PEG_Ratio', 'N/A',
            '<0.5 exceptional OK | 0.5-1.0 strong OK | 1-2 WARN | >2 BAD',
            S_BAD, PENDING, 'yfinance info earningsGrowth',
            'PIPELINE PENDING — earningsGrowth unavailable. Cannot compute PEG.')

    if eg <= 0:
        return kpi_row(t, 'PEG_Ratio', 'N/A (negative growth)',
            '<0.5 exceptional OK | 0.5-1.0 strong OK | 1-2 WARN | >2 BAD',
            S_BAD, TRIANGULATED if pe_proxy else DIRECT,
            'yfinance info trailingPE|forwardPE + earningsGrowth',
            f'Negative growth rate ({eg*100:.1f}%) — PEG not meaningful. PE={pe:.1f}')

    peg    = pe / (eg * 100)
    status = S_OK if peg < 1.0 else (S_WARN if peg < 2.0 else S_BAD)
    conf   = TRIANGULATED if pe_proxy else DIRECT
    note   = (f'{"forwardPE used as proxy — " if pe_proxy else ""}PE={pe:.1f}, '
              f'EarningsGrowth={eg*100:.1f}%')
    return kpi_row(t, 'PEG_Ratio', f"{peg:.2f}",
        '<0.5 exceptional OK | 0.5-1.0 strong OK | 1-2 WARN | >2 BAD',
        status, conf, 'trailingPE|forwardPE + earningsGrowth (yfinance info)', note)


def kpi_current_ratio(d: dict) -> dict:
    t  = d['symbol']
    bs = d['balance']

    ca = sv(bs, 'Current Assets')
    cl = sv(bs, 'Current Liabilities')

    if ca is None or cl is None or cl == 0:
        return kpi_row(t, 'Current_Ratio', 'N/A',
            '1.5-2.0 ideal OK | 1.0-1.5 WARN | <1.0 BAD | >3.0 overexpansion WARN',
            S_NA, PENDING, 'yfinance balance_sheet',
            f'PIPELINE PENDING — ca={ca is not None}, cl={cl is not None}')

    cr = ca / cl
    if 1.5 <= cr <= 3.0:  status = S_OK
    elif 1.0 <= cr < 1.5: status = S_WARN
    elif cr > 3.0:        status = S_WARN
    else:                 status = S_BAD

    note = (f'CA={fmt_money(ca)}, CL={fmt_money(cl)}. '
            + ('Over-expansion warning (>3.0). ' if cr > 3.0 else '')
            + ('Red flag — liquidity risk (<1.0). ' if cr < 1.0 else ''))
    return kpi_row(t, 'Current_Ratio', f"{cr:.2f}x",
        '1.5-2.0 ideal OK | 1.0-1.5 WARN | <1.0 BAD | >3.0 overexpansion WARN',
        status, DIRECT, 'Current Assets + Current Liabilities (yfinance balance_sheet)', note.strip())


def kpi_burn_multiple(d: dict) -> dict:
    t    = d['symbol']
    info = d['info']
    cf   = d['cashflow']
    inc  = d['income']

    if t not in BURN_TICKERS:
        return kpi_row(t, 'Net_Burn_Multiple', 'N/A — profitable entity',
            '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD', S_NA, DIRECT,
            'N/A', 'Metric applies to pre-profit companies only.')

    ocf   = sv(cf, 'Operating Cash Flow') or info_val(info, 'operatingCashflow')
    rev_0 = sv(inc, 'Total Revenue', 'Operating Revenue', col=0)
    rev_1 = sv(inc, 'Total Revenue', 'Operating Revenue', col=1)

    if ocf is None:
        return kpi_row(t, 'Net_Burn_Multiple', 'N/A', '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD',
            S_NA, PENDING, 'yfinance cashflow',
            'PIPELINE PENDING — operating cashflow unavailable.')

    if ocf >= 0:
        return kpi_row(t, 'Net_Burn_Multiple', 'N/A — cash flow positive',
            '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD', S_OK, DIRECT,
            'Operating Cash Flow (yfinance cashflow)', f'OCF={fmt_money(ocf)} — positive, no burn.')

    if rev_0 is None or rev_1 is None:
        return kpi_row(t, 'Net_Burn_Multiple', 'N/A', '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD',
            S_NA, PENDING, 'yfinance income_stmt (2yr)',
            'PIPELINE PENDING — insufficient revenue history for ARR proxy.')

    arr_proxy = rev_0 - rev_1
    if arr_proxy <= 0:
        return kpi_row(t, 'Net_Burn_Multiple', 'N/A (revenue declining)',
            '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD', S_BAD, TRIANGULATED,
            'yfinance cashflow + income_stmt (2yr)',
            f'Revenue YoY delta <= 0 ({fmt_money(rev_0)} vs {fmt_money(rev_1)}) — ARR proxy not meaningful.')

    bm     = abs(ocf) / arr_proxy
    status = S_OK if bm < 1.0 else (S_WARN if bm <= 1.5 else S_BAD)
    return kpi_row(t, 'Net_Burn_Multiple', f"{bm:.2f}x",
        '<1.0 OK | 1.0-1.5 WARN | >1.5 BAD', status, TRIANGULATED,
        'Operating Cash Flow + Total Revenue YoY delta (yfinance cashflow/income_stmt)',
        f'TRIANGULATED — true ARR not in yfinance. '
        f'NetOutflow={fmt_money(abs(ocf))}, ARR proxy={fmt_money(arr_proxy)} '
        f'(rev {fmt_money(rev_0)} - {fmt_money(rev_1)}). PIPELINE PENDING for true ARR.')


def kpi_nrr(d: dict) -> dict:
    t = d['symbol']
    if t not in SUB_TICKERS:
        return kpi_row(t, 'Net_Revenue_Retention', 'N/A — non-subscription model',
            '>115% enterprise OK | >105% mid-market OK | 100-115% WARN | <100% BAD',
            S_NA, DIRECT, 'N/A',
            'Metric applies to subscription/SaaS companies only.')

    return kpi_row(t, 'Net_Revenue_Retention',
        'PIPELINE PENDING — SEC EDGAR + earnings transcripts needed',
        '>115% enterprise OK | >105% mid-market OK | 100-115% WARN | <100% BAD',
        S_NA, PENDING, 'SEC EDGAR 10-K / earnings call transcripts',
        'Formula: (Beginning ARR + Expansion - Contraction - Churn) / Beginning ARR x 100. '
        'ARR components not disclosed in yfinance. Flag for manual update each quarter.')


def kpi_revenue_per_token(d: dict) -> dict:
    t    = d['symbol']
    inc  = d['income']
    info = d['info']

    rev = sv(inc, 'Total Revenue', 'Operating Revenue') or info_val(info, 'totalRevenue')
    if rev is None:
        return kpi_row(t, 'Revenue_per_1M_Tokens', 'N/A',
            'Higher = better AI monetization efficiency | >20% QoQ decline = WARN',
            S_NA, PENDING, 'yfinance income_stmt',
            'PIPELINE PENDING — total revenue unavailable.')

    rd = sv(inc, 'Research And Development') or info_val(info, 'researchDevelopment')
    if rd is None or rd <= 0:
        return kpi_row(t, 'Revenue_per_1M_Tokens', 'N/A',
            'Higher = better AI monetization efficiency | >20% QoQ decline = WARN',
            S_NA, PENDING, 'yfinance income_stmt (Research And Development)',
            'PIPELINE PENDING — R&D spend unavailable. Cannot apply Tier 4 floor. '
            'Check SEC EDGAR for Tier 1/2/3 AI spend disclosure.')

    # Tier 4: 5% of R&D as conservative AI spend floor
    ai_spend     = rd * 0.05
    token_millions = ai_spend / AI_RATE   # millions of tokens this spend buys
    rev_per_1m   = rev / token_millions

    # QoQ check
    prior = get_prior_kpi9(t)
    if prior and prior > 0:
        qoq_chg = (rev_per_1m - prior) / prior * 100
        qoq_str = f"{qoq_chg:+.1f}% QoQ" + (' — WARN: >20% decline' if qoq_chg < -20 else '')
    else:
        qoq_str = 'N/A — first snapshot'

    note = (
        f"Annual Revenue: {fmt_money(rev)} | "
        f"Documented AI Spend: {fmt_money(ai_spend)} "
        f"(Tier 4 ASSUMED — 5% of R&D {fmt_money(rd)}, date: {TODAY[:4]}) | "
        f"Token equivalent: {token_millions:.1f}M tokens at ${AI_RATE}/1M "
        f"(blended: 70% input@$2.50 + 30% output@$10.00 across OpenAI/Anthropic/Google/AWS/Azure) | "
        f"Revenue per 1M Tokens: {fmt_money(rev_per_1m)} | "
        f"QoQ change: {qoq_str} | "
        f"PIPELINE PENDING — SEC EDGAR needed for Tier 1/2; earnings call quote for Tier 3. "
        f"Never use single-vendor pricing as denominator."
    )
    return kpi_row(t, 'Revenue_per_1M_Tokens', fmt_money(rev_per_1m),
        'Higher = better AI monetization efficiency | >20% QoQ decline = WARN',
        S_WARN, ASSUMED,
        f'Total Revenue / (5% R&D / ${AI_RATE} per 1M tokens) — yfinance income_stmt', note)


def run_all_kpis(data_map: dict) -> list:
    rows = []
    fns  = [kpi_return_on_capital, kpi_fcf_margin, kpi_gross_margin,
            kpi_earnings_yield, kpi_peg, kpi_current_ratio,
            kpi_burn_multiple, kpi_nrr, kpi_revenue_per_token]
    for ticker in TICKERS:
        d = data_map.get(ticker, {'symbol': ticker, 'info': {}, 'income': pd.DataFrame(),
                                   'cashflow': pd.DataFrame(), 'balance': pd.DataFrame()})
        for fn in fns:
            try:
                rows.append(fn(d))
            except Exception as e:
                rows.append(kpi_row(ticker, fn.__name__, 'ERROR', '', S_BAD, PENDING, 'yfinance', str(e)))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — AI WALLET SHARE COMPUTE
# ─────────────────────────────────────────────────────────────────────────────
def run_wallet_share(data_map: dict) -> list:
    rows = []
    for ticker, segs in WALLET_SEGMENTS.items():
        d         = data_map.get(ticker, {})
        info      = d.get('info', {})
        inc       = d.get('income', pd.DataFrame())
        total_rev = sv(inc, 'Total Revenue', 'Operating Revenue') or info_val(info, 'totalRevenue')

        for seg_type, seg in segs.items():
            method = seg['method']
            if method == 'hardcoded':
                rev = seg['hardcoded_rev']
            elif method == 'yfinance_total':
                rev = total_rev
            else:
                rev = None

            tam    = seg['tam']
            wallet = (rev / tam * 100) if (rev is not None and tam) else None
            prior  = get_prior_wallet_share(ticker, seg_type)
            qoq    = round(wallet - prior, 4) if (wallet is not None and prior is not None) else None
            notes  = seg['notes']
            if qoq is not None and abs(qoq) > 2:
                notes += f' | SIGNIFICANT REALLOCATION: {qoq:+.4f}pp QoQ swing.'

            rows.append({
                'ticker': ticker, 'segment_type': seg_type,
                'segment_name': seg['segment_name'],
                'company_revenue': rev, 'market_tam': tam,
                'wallet_share_pct': round(wallet, 6) if wallet is not None else None,
                'prior_wallet_share_pct': prior, 'qoq_change_pp': qoq,
                'confidence_tier': seg['confidence'],
                'tam_source': seg['tam_source'], 'tam_date': seg['tam_date'],
                'notes': notes, 'timestamp': TODAY,
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — AI COMMITMENT TRACKER SEED
# ─────────────────────────────────────────────────────────────────────────────
def run_commitment_tracker() -> list:
    rows = []
    for ticker, products in AI_COMMITMENT_SEEDS.items():
        for p in products:
            rows.append({
                'ticker': ticker,
                'product_name': p['product_name'],
                'signal_type': p['signal_type'],
                'signal_date': p['signal_date'],
                'source': p['source'],
                'adoption_metric': p['adoption_metric'],
                'adoption_value': p['adoption_value'],
                'status': STATUS_DISPLAY.get(p['status'], p['status']),
                'notes': p['notes'],
                'timestamp': TODAY,
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PART 4 — OUTPUT GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

class Tee:
    def __init__(self, path: Path):
        self._f = open(path, 'w', encoding='utf-8', errors='replace')

    def w(self, *lines):
        for line in lines:
            print(line)
            self._f.write(line + '\n')

    def close(self):
        self._f.close()


def output_kpi_report(kpi_rows: list):
    path = REPORT_DIR / 'kpi_audit_report.txt'
    tee  = Tee(path)
    SEP  = '─' * 200

    tee.w('', '=' * 200)
    tee.w('  OUTPUT 1 — KPI COVERAGE TABLE')
    tee.w(f'  {REPORT_HEADER}')
    tee.w('=' * 200)
    tee.w(f"  {'Ticker':<7} {'KPI':<28} {'Value':<22} {'Status':<7} {'Confidence':<22}"
          f" {'Threshold':<55} {'Notes'}")
    tee.w(SEP)

    cur = None
    for r in kpi_rows:
        if r['ticker'] != cur:
            if cur: tee.w(SEP)
            cur = r['ticker']

        st   = STATUS_ICON.get(r['status'], r['status'])
        conf = CONF_ICON.get(r['confidence_tier'], r['confidence_tier'])
        note = (r['notes'] or '')[:120]

        tee.w(f"  {r['ticker']:<7} {r['kpi_name']:<28} {str(r['value']):<22} "
              f"{st:<7} {conf:<22} {str(r['threshold']):<55} {note}")

    tee.w(SEP, '')
    tee.close()
    print(f"\n  >> Saved: {path}")


def output_wallet_report(wallet_rows: list):
    path = REPORT_DIR / 'ai_wallet_share_report.txt'
    tee  = Tee(path)
    SEP  = '─' * 200

    tee.w('', '=' * 200)
    tee.w('  OUTPUT 2 — AI WALLET SHARE TABLE')
    tee.w(f'  {REPORT_HEADER}')
    tee.w('=' * 200)
    tee.w(f"  {'Ticker':<7} {'Seg':<9} {'Segment Name':<48} {'Revenue':>14} {'TAM':>16}"
          f" {'Wallet%':>10} {'Prior%':>10} {'QoQ pp':>10} {'Conf':<22} {'Source':<16} {'Date'}")
    tee.w(SEP)

    cur = None
    for r in wallet_rows:
        if r['ticker'] != cur:
            if cur: tee.w(SEP)
            cur = r['ticker']

        rev_s   = fmt_money(r['company_revenue']) if r['company_revenue'] is not None else 'PENDING'
        tam_s   = fmt_money(r['market_tam'])
        ws_s    = f"{r['wallet_share_pct']:.4f}%" if r['wallet_share_pct'] is not None else 'PENDING'
        prior_s = f"{r['prior_wallet_share_pct']:.4f}%" if r['prior_wallet_share_pct'] is not None else 'N/A — 1st run'
        qoq_s   = f"{r['qoq_change_pp']:+.4f}pp" if r['qoq_change_pp'] is not None else 'N/A — 1st run'
        conf    = CONF_ICON.get(r['confidence_tier'], r['confidence_tier'])

        tee.w(f"  {r['ticker']:<7} {r['segment_type']:<9} {r['segment_name'][:47]:<48}"
              f" {rev_s:>14} {tam_s:>16} {ws_s:>10} {prior_s:>10} {qoq_s:>10}"
              f" {conf:<22} {r['tam_source']:<16} {r['tam_date']}")
        tee.w(f"    Notes: {r['notes'][:160]}")

    tee.w(SEP, '')
    tee.close()
    print(f"  >> Saved: {path}")


def output_commitment_report(commit_rows: list):
    path = REPORT_DIR / 'ai_commitment_report.txt'
    tee  = Tee(path)
    SEP  = '─' * 160

    tee.w('', '=' * 160)
    tee.w('  OUTPUT 3 — AI COMMITMENT TRACKER')
    tee.w(f'  {REPORT_HEADER}')
    tee.w('=' * 160)

    cur = None
    for r in commit_rows:
        if r['ticker'] != cur:
            if cur: tee.w('')
            tee.w(SEP, f"  {r['ticker']} — AI Product Basket", SEP)
            cur = r['ticker']

        tee.w(f"  {r['status']:<20}  {r['product_name']}")
        tee.w(f"    Signal type   : {r['signal_type']}")
        tee.w(f"    Signal date   : {r['signal_date']}")
        tee.w(f"    Source        : {r['source'][:140]}")
        tee.w(f"    Metric        : {r['adoption_metric']}")
        tee.w(f"    Value         : {r['adoption_value'][:140]}")
        tee.w(f"    Notes         : {r['notes'][:160]}")
        tee.w('')

    tee.w(SEP, '')
    tee.close()
    print(f"  >> Saved: {path}")


def output_red_flags(kpi_rows: list, wallet_rows: list, commit_rows: list):
    path = REPORT_DIR / 'red_flag_summary.txt'
    tee  = Tee(path)
    SEP  = '=' * 120

    tee.w('', SEP, '  OUTPUT 4 — RED FLAG SUMMARY', f'  {REPORT_HEADER}', SEP)
    found = False

    for ticker in TICKERS:
        t_kpis   = [r for r in kpi_rows   if r['ticker'] == ticker]
        t_wallet = [r for r in wallet_rows if r['ticker'] == ticker]
        t_commit = [r for r in commit_rows if r['ticker'] == ticker]

        bad_kpis = [r for r in t_kpis if r['status'] == S_BAD]
        bb       = next((r for r in t_wallet if r['segment_type'] == 'BIG_BET'), None)
        primary  = t_commit[0] if t_commit else None
        tok_kpi  = next((r for r in t_kpis if r['kpi_name'] == 'Revenue_per_1M_Tokens'), None)

        flags = []

        if len(bad_kpis) >= 3:
            flags.append(f"  3+ KPIs flagged BAD: {', '.join(r['kpi_name'] for r in bad_kpis)}")

        if bb and bb.get('qoq_change_pp') is not None and bb['qoq_change_pp'] < 0:
            flags.append(f"  Big Bet AI Wallet Share declining QoQ: {bb['qoq_change_pp']:+.4f}pp ({bb['segment_name']})")

        if primary and primary['status'] in ('🔴 HYPE', '❌ FAILED'):
            flags.append(f"  Primary AI product status: {primary['status']} — {primary['product_name']}")

        if tok_kpi and tok_kpi.get('notes') and 'WARN: >20% decline' in tok_kpi['notes']:
            flags.append(f"  Revenue per 1M Tokens declined >20% QoQ")

        if flags:
            found = True
            tee.w(f"\n  TICKER: {ticker} — {len(flags)} RED FLAG(S) TRIGGERED")
            for f in flags:
                tee.w(f)

    if not found:
        tee.w('\n  No red flags triggered on this snapshot.')
        tee.w('  Note: QoQ comparisons require a prior run in the database.')
        tee.w('  Re-run next quarter to enable QoQ delta analysis.')

    tee.w('', SEP, '')
    tee.close()
    print(f"  >> Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# PART 5 — GITHUB ISSUES CHECK / CREATE
# ─────────────────────────────────────────────────────────────────────────────
def sync_github_issues():
    print('\n── Part 5: GitHub Issues ───────────────────────────────────────────────')

    token = None
    try:
        proc = subprocess.Popen(
            ['git', 'credential', 'fill'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, _ = proc.communicate(b'protocol=https\nhost=github.com\n\n', timeout=8)
        for line in out.decode(errors='replace').splitlines():
            if line.startswith('password='):
                token = line.split('=', 1)[1].strip()
                break
    except Exception as e:
        print(f"  [WARN] Could not retrieve GitHub token via git credential: {e}")

    if not token:
        print("  [SKIP] No GitHub token found. Skipping issue sync.")
        return

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }
    api = f'https://api.github.com/repos/{GITHUB_REPO}/issues'

    try:
        resp = requests.get(api, headers=headers, params={'state': 'all', 'per_page': 100}, timeout=10)
        existing = {i['title'].strip().lower() for i in resp.json() if isinstance(i, dict) and 'title' in i}
    except Exception as e:
        print(f"  [ERROR] Could not fetch existing issues: {e}")
        return

    created = skipped = 0
    for issue in REQUIRED_ISSUES:
        if issue['title'].strip().lower() in existing:
            print(f"  [EXISTS]  {issue['title'][:80]}")
            skipped += 1
        else:
            try:
                r = requests.post(api, headers=headers, timeout=10,
                                  json={'title': issue['title'], 'body': issue['body'],
                                        'labels': ['enhancement']})
                if r.status_code == 201:
                    num = r.json().get('number', '?')
                    print(f"  [CREATED] #{num} — {issue['title'][:75]}")
                    created += 1
                else:
                    print(f"  [ERROR]   {r.status_code}: {r.text[:120]}")
            except Exception as e:
                print(f"  [ERROR]   {issue['title'][:60]}: {e}")

    print(f"\n  GitHub: {created} created, {skipped} already exist.")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print('\n' + '=' * 80)
    print('  auto-invest-os-zen — Full KPI Audit Pipeline')
    print(f'  {TODAY}')
    print('=' * 80)

    create_tables()

    print('\n── Fetching yfinance data ───────────────────────────────────────────────')
    data_map = {ticker: fetch_data(ticker) for ticker in TICKERS}

    print('\n── Part 1: Computing 9 KPIs × 10 tickers ───────────────────────────────')
    kpi_rows = run_all_kpis(data_map)
    save_kpi(kpi_rows)
    print(f'  {len(kpi_rows)} KPI records → DB')

    print('\n── Part 2: AI Wallet Share ──────────────────────────────────────────────')
    wallet_rows = run_wallet_share(data_map)
    save_wallet(wallet_rows)
    print(f'  {len(wallet_rows)} wallet share records → DB')

    print('\n── Part 3: AI Commitment Tracker ────────────────────────────────────────')
    commit_rows = run_commitment_tracker()
    save_commitment(commit_rows)
    print(f'  {len(commit_rows)} commitment records → DB')

    print('\n── Part 4: Generating 4 reports ─────────────────────────────────────────')
    output_kpi_report(kpi_rows)
    output_wallet_report(wallet_rows)
    output_commitment_report(commit_rows)
    output_red_flags(kpi_rows, wallet_rows, commit_rows)

    sync_github_issues()

    print('\n' + '=' * 80)
    print(f'  Audit complete. Reports: {REPORT_DIR}')
    print(f'  Database: {DB_PATH}')
    print('=' * 80 + '\n')


if __name__ == '__main__':
    main()
