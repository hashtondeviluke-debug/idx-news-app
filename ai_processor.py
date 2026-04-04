"""
IDX News Intelligence System - Step 2: AI Intelligence & Sentiment Processor
=============================================================================
Reads raw_news.json produced by main_scraper.py, runs each article through
Gemini 2.0 Flash (free tier), and appends structured AI analysis back into
the data, producing analyzed_news.json.

Analysis produced per article:
  - summary       : 1-2 sentence plain-language summary
  - impact_score  : 1 (Very Bearish) → 5 (Very Bullish) for IDX
  - impact_label  : human-readable label for the score
  - category      : Corporate Action | Financial Result | Macro Economy |
                    Regulatory | Market Movement | Other
  - target_emiten : list of IDX stock codes found (e.g. ["BBCA", "GOTO"])
  - confidence    : high | medium | low (AI self-assessed)

Usage:
    python ai_processor.py
    python ai_processor.py --dry-run          # Show prompt/response without saving
    python ai_processor.py --reprocess        # Re-analyze already-processed articles
    python ai_processor.py --input custom.json --output custom_analyzed.json

Dependencies: google-generativeai, python-dotenv
Install: pip install -r requirements.txt

API Key: Copy .env.example → .env and fill in GEMINI_API_KEY
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Load .env before any other imports that might need env vars ──────────────
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
except ImportError:
    pass  # Will show a clear error below if key is missing

try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    from google.api_core.exceptions import (
        ResourceExhausted,   # 429 — quota hit
        ServiceUnavailable,  # 503 — transient server error
        GoogleAPIError,
    )
except ImportError:
    print(
        "\n[ERROR] google-generativeai is not installed.\n"
        "Run: pip install -r requirements.txt\n"
    )
    sys.exit(1)

from rate_limiter import RateLimiter, exponential_backoff

# ─────────────────────────── CONFIG ────────────────────────────────────────

BASE_DIR    = Path(__file__).parent
INPUT_FILE  = BASE_DIR / "raw_news.json"
OUTPUT_FILE = BASE_DIR / "analyzed_news.json"
LOG_FILE    = BASE_DIR / "ai_processor.log"

# Gemini model — gemini-2.0-flash is free tier and extremely fast
GEMINI_MODEL  = "gemini-2.0-flash"
MAX_RETRIES   = 4          # total attempts per article (1 original + 3 retries)
BODY_CHAR_CAP = 4000       # truncate body sent to Gemini to save tokens
MIN_BODY_CHARS = 100       # skip articles with too little content

IMPACT_LABELS = {
    1: "Very Bearish",
    2: "Bearish",
    3: "Neutral",
    4: "Bullish",
    5: "Very Bullish",
}

VALID_CATEGORIES = {
    "Corporate Action",
    "Financial Result",
    "Macro Economy",
    "Regulatory",
    "Market Movement",
    "Other",
}

# ─────────────────────────── LOGGING ───────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── GEMINI SETUP ──────────────────────────────────

def init_gemini() -> genai.GenerativeModel:
    """Initialise and return the Gemini client. Exits on missing API key."""
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        logger.error(
            "GEMINI_API_KEY not set.\n"
            "  1. Copy .env.example to .env\n"
            "  2. Add your key: GEMINI_API_KEY=your_key_here\n"
            "  Get a free key at: https://aistudio.google.com/apikey"
        )
        sys.exit(1)

    genai.configure(api_key=api_key)

    # Safety settings — relaxed for financial news (no harmful content expected,
    # but default BLOCK_LOW_AND_ABOVE can false-positive on market crash headlines)
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT:        HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH:       HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }

    generation_config = genai.GenerationConfig(
        temperature=0.1,           # low temperature → deterministic, factual output
        response_mime_type="application/json",  # request JSON directly
        max_output_tokens=512,     # analysis is short; cap to save quota
    )

    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        generation_config=generation_config,
        safety_settings=safety_settings,
    )
    logger.info(f"Gemini model initialised: {GEMINI_MODEL}")
    return model


# ─────────────────────────── PROMPT BUILDER ────────────────────────────────

SYSTEM_PROMPT = """You are an expert Indonesian stock market analyst specialising in the Indonesia Stock Exchange (IDX/BEI).
Your job is to analyze news articles and extract structured intelligence for equity traders and investors.

When given an article, respond ONLY with a valid JSON object — no markdown, no explanation, no code fences.

Required JSON schema:
{
  "summary": "<1-2 sentence summary in English, focusing on IDX/market implications>",
  "impact_score": <integer 1-5>,
  "impact_label": "<one of: Very Bearish | Bearish | Neutral | Bullish | Very Bullish>",
  "category": "<one of: Corporate Action | Financial Result | Macro Economy | Regulatory | Market Movement | Other>",
  "target_emiten": [<IDX stock codes as strings, e.g. "BBCA", "GOTO", or empty array []>],
  "confidence": "<one of: high | medium | low>"
}

Impact score guide:
  1 = Very Bearish  — strong negative signal (fraud, major loss, delisting risk, sanctions)
  2 = Bearish       — moderately negative (earnings miss, rating downgrade, sector headwinds)
  3 = Neutral       — no clear directional signal or mixed signals
  4 = Bullish       — moderately positive (strong earnings, new contracts, analyst upgrade)
  5 = Very Bullish  — strong positive signal (record profit, major acquisition win, index inclusion)

Category definitions:
  Corporate Action  — dividends, rights issues, stock splits, mergers & acquisitions, IPO
  Financial Result  — quarterly/annual earnings, revenue, profit/loss reports
  Macro Economy     — BI interest rates, GDP, inflation, government policy, global macro
  Regulatory        — OJK rules, BEI regulations, SEC/CFTC equivalent actions
  Market Movement   — IHSG index moves, sector rotations, trading halts, foreign flows
  Other             — anything not fitting the above

Stock code rules:
  - Use only official IDX stock codes (4-letter codes like BBCA, GOTO, TLKM, ASII)
  - If no specific emiten is mentioned, return []
  - Do not invent codes; only extract clearly mentioned ones

Confidence:
  high   = article provides clear, specific facts
  medium = article is somewhat vague or inferential
  low    = article is very general or lacks specific data"""


def build_user_prompt(title: str, body: str) -> str:
    """Build the per-article user prompt with truncated body to save tokens."""
    truncated_body = body[:BODY_CHAR_CAP]
    if len(body) > BODY_CHAR_CAP:
        truncated_body += "\n[... article truncated for analysis ...]"
    return f"Article Title: {title}\n\nArticle Body:\n{truncated_body}"


# ─────────────────────────── RESPONSE PARSER ───────────────────────────────

def parse_gemini_response(raw_text: str, article_url: str) -> Optional[dict]:
    """
    Parse and validate Gemini's JSON response.
    Returns a clean dict or None if parsing fails.
    """
    # Strip any accidental markdown code fences
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error for {article_url}: {e}")
        logger.debug(f"Raw response: {raw_text[:500]}")
        return None

    # ── Validate and coerce fields ──────────────────────────────────────────

    # impact_score: must be int 1-5
    score = data.get("impact_score")
    try:
        score = int(score)
        if not 1 <= score <= 5:
            score = 3
    except (TypeError, ValueError):
        score = 3
    data["impact_score"] = score

    # impact_label: derive from score if missing/wrong
    data["impact_label"] = IMPACT_LABELS.get(score, "Neutral")

    # category: must be one of VALID_CATEGORIES
    category = data.get("category", "Other")
    if category not in VALID_CATEGORIES:
        category = "Other"
    data["category"] = category

    # target_emiten: must be a list of uppercase strings, max 4 chars each
    emiten = data.get("target_emiten", [])
    if not isinstance(emiten, list):
        emiten = []
    # Sanitise: uppercase, strip, 2-4 alpha chars only (IDX codes are 4 letters)
    emiten = [
        e.strip().upper() for e in emiten
        if isinstance(e, str) and 2 <= len(e.strip()) <= 6 and e.strip().isalpha()
    ]
    data["target_emiten"] = emiten

    # summary: must be non-empty string
    summary = data.get("summary", "").strip()
    if not summary:
        summary = "No summary available."
    data["summary"] = summary

    # confidence: must be one of valid values
    confidence = data.get("confidence", "medium").lower()
    if confidence not in ("high", "medium", "low"):
        confidence = "medium"
    data["confidence"] = confidence

    return data


# ─────────────────────────── CORE ANALYSER ─────────────────────────────────

def analyze_article(
    article: dict,
    model: genai.GenerativeModel,
    limiter: RateLimiter,
    dry_run: bool,
) -> Optional[dict]:
    """
    Run a single article through Gemini. Returns the analysis dict or None.
    Handles rate limiting and retries with exponential back-off.
    """
    title = article.get("title", "")
    body  = article.get("body", "")
    url   = article.get("url", "")

    if len(body) < MIN_BODY_CHARS:
        logger.debug(f"  Skipping (body too short): {url}")
        return None

    user_prompt = build_user_prompt(title, body)

    if dry_run:
        logger.info(f"  [DRY RUN] Would send prompt ({len(user_prompt)} chars) for: {title[:60]}")
        # Return a stub analysis so the pipeline can still be tested end-to-end
        return {
            "summary": "[DRY RUN] Analysis not performed.",
            "impact_score": 3,
            "impact_label": "Neutral",
            "category": "Other",
            "target_emiten": [],
            "confidence": "low",
        }

    # ── Retry loop ───────────────────────────────────────────────────────────
    for attempt in range(MAX_RETRIES):
        if limiter.daily_quota_exhausted():
            return None

        limiter.wait_if_needed()

        try:
            response = model.generate_content(
                contents=[
                    {"role": "user", "parts": [SYSTEM_PROMPT]},
                    {"role": "model", "parts": ['{"understood": true}']},  # priming turn
                    {"role": "user", "parts": [user_prompt]},
                ]
            )

            raw_text = response.text
            result   = parse_gemini_response(raw_text, url)

            if result:
                return result

            # If parsing failed, retry once with a nudge
            logger.warning(f"  Parse failed on attempt {attempt+1}. Retrying...")

        except ResourceExhausted:
            # 429: quota limit hit mid-run
            wait = exponential_backoff(attempt, base=15.0, cap=180.0)
            logger.warning(
                f"  429 Rate limit hit (attempt {attempt+1}/{MAX_RETRIES}). "
                f"Backing off {wait:.0f}s ..."
            )
            time.sleep(wait)

        except ServiceUnavailable:
            wait = exponential_backoff(attempt, base=5.0, cap=60.0)
            logger.warning(
                f"  503 Service unavailable (attempt {attempt+1}/{MAX_RETRIES}). "
                f"Retrying in {wait:.0f}s ..."
            )
            time.sleep(wait)

        except GoogleAPIError as e:
            logger.error(f"  Gemini API error on attempt {attempt+1}: {e}")
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(exponential_backoff(attempt))

        except Exception as e:
            logger.error(f"  Unexpected error on attempt {attempt+1}: {e}", exc_info=True)
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(2)

    logger.error(f"  Exhausted {MAX_RETRIES} attempts for: {url}")
    return None


# ─────────────────────────── I/O HELPERS ───────────────────────────────────

def load_raw_news(path: Path) -> list[dict]:
    if not path.exists():
        logger.error(
            f"Input file not found: {path}\n"
            "Run main_scraper.py first to generate raw_news.json"
        )
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        logger.error("raw_news.json must contain a JSON array at the top level.")
        sys.exit(1)
    return data


def load_existing_output(path: Path) -> dict[str, dict]:
    """Load previously analyzed articles keyed by URL for fast lookup."""
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)
        return {item["url"]: item for item in items if "url" in item}
    except (json.JSONDecodeError, IOError):
        logger.warning(f"Could not read existing {path.name} — starting fresh.")
        return {}


def save_output(articles: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(articles)} articles to {path.name}")


# ─────────────────────────── ENTRY POINT ───────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="IDX News AI Processor (Gemini)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be processed without calling the Gemini API"
    )
    parser.add_argument(
        "--reprocess", action="store_true",
        help="Re-run AI analysis on articles already in analyzed_news.json"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_FILE),
        help=f"Path to input JSON file (default: {INPUT_FILE.name})"
    )
    parser.add_argument(
        "--output", type=str, default=str(OUTPUT_FILE),
        help=f"Path to output JSON file (default: {OUTPUT_FILE.name})"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only N articles (useful for testing)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_path  = Path(args.input)
    output_path = Path(args.output)

    logger.info("=" * 60)
    logger.info("IDX News AI Processor — starting run")
    logger.info(f"  Model  : {GEMINI_MODEL} (free tier)")
    logger.info(f"  Mode   : {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ── Load data ────────────────────────────────────────────────────────────
    raw_articles     = load_raw_news(input_path)
    existing_results = load_existing_output(output_path) if not args.reprocess else {}

    logger.info(f"Raw articles loaded     : {len(raw_articles)}")
    logger.info(f"Already analyzed        : {len(existing_results)}")

    # Determine which articles need processing
    pending = [a for a in raw_articles if a.get("url") not in existing_results]
    if args.limit:
        pending = pending[:args.limit]

    logger.info(f"Articles to process     : {len(pending)}")

    if not pending:
        logger.info("Nothing new to process. Exiting.")
        # Still write the merged output so the file is always up to date
        all_articles = list(existing_results.values())
        save_output(all_articles, output_path)
        return

    # ── Init Gemini (skipped in dry-run so no key needed) ────────────────────
    model   = None if args.dry_run else init_gemini()
    limiter = RateLimiter()

    # ── Process articles ─────────────────────────────────────────────────────
    success_count = 0
    error_count   = 0

    for idx, article in enumerate(pending, start=1):
        title = article.get("title", "")[:60]
        url   = article.get("url", "")
        logger.info(f"[{idx}/{len(pending)}] {title}")

        analysis = analyze_article(article, model, limiter, args.dry_run)

        if analysis:
            enriched = {
                **article,                       # original fields
                "ai_analysis": analysis,         # nested analysis block
                "ai_model":    GEMINI_MODEL,
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
            }
            existing_results[url] = enriched
            success_count += 1

            # Pretty-print result in live mode
            if not args.dry_run:
                logger.info(
                    f"  ✓ Score={analysis['impact_score']} ({analysis['impact_label']}) "
                    f"| {analysis['category']} "
                    f"| Emiten={analysis['target_emiten'] or 'none detected'}"
                )
        else:
            # Keep the original article without analysis so we don't lose data
            enriched = {
                **article,
                "ai_analysis": None,
                "ai_model":    GEMINI_MODEL,
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
                "ai_error":    "Analysis failed after max retries",
            }
            existing_results[url] = enriched
            error_count += 1
            logger.warning(f"  ✗ Analysis failed for: {url}")

        # Save incrementally every 10 articles to guard against interruption
        if idx % 10 == 0 and not args.dry_run:
            save_output(list(existing_results.values()), output_path)
            logger.info(f"  [checkpoint] Saved progress ({idx}/{len(pending)} processed)")

    # ── Final save ───────────────────────────────────────────────────────────
    all_articles = list(existing_results.values())

    if not args.dry_run:
        save_output(all_articles, output_path)
    else:
        logger.info("[DRY RUN] Output not written to disk.")

    logger.info("=" * 60)
    logger.info(f"Run complete.")
    logger.info(f"  Processed : {len(pending)}")
    logger.info(f"  Success   : {success_count}")
    logger.info(f"  Failed    : {error_count}")
    logger.info(f"  API calls : {limiter.daily_count}")
    logger.info(f"  Total in output: {len(all_articles)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
