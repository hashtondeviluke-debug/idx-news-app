"""
IDX News Intelligence System - Step 1: Scraper & Source Manager
================================================================
Reads sources.json, scrapes listed news portals, filters IDX-relevant
articles, deduplicates, and saves clean results to raw_news.json.

Usage:
    python main_scraper.py
    python main_scraper.py --dry-run        # Preview articles without saving
    python main_scraper.py --reset-db       # Clear deduplication history
    python main_scraper.py --source "Kontan" # Scrape only one source

Dependencies: requests, beautifulsoup4, lxml
Install: pip install -r requirements.txt
"""

import json
import logging
import os
import re
import sqlite3
import time
import argparse
import hashlib
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ─────────────────────────── CONFIG ────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(BASE_DIR, "sources.json")
OUTPUT_FILE  = os.path.join(BASE_DIR, "raw_news.json")
DB_FILE      = os.path.join(BASE_DIR, "processed_news.db")
LOG_FILE     = os.path.join(BASE_DIR, "scraper.log")

REQUEST_TIMEOUT = 20          # seconds per HTTP request
MAX_ARTICLES_PER_SOURCE = 30  # cap per source per run
MIN_BODY_LENGTH = 150         # skip articles whose body is too short (chars)

# Rotate through these User-Agents to reduce bot-detection risk
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

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

# ─────────────────────────── DATABASE ──────────────────────────────────────

class DeduplicationDB:
    """SQLite-backed store for processed article URLs.
    
    Using SQLite instead of a plain .log file gives us:
    - Indexed lookups (O(log n) instead of O(n))
    - Atomic writes (no corruption on crash mid-write)
    - Easy querying for future analytics steps
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")  # safe concurrent writes
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_articles (
                    url_hash    TEXT PRIMARY KEY,
                    url         TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    title       TEXT,
                    scraped_at  TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scraped_at ON processed_articles(scraped_at);")
        logger.debug(f"Deduplication DB ready: {self.db_path}")

    @staticmethod
    def _hash(url: str) -> str:
        """Normalise URL before hashing to handle trailing-slash variants."""
        clean = url.strip().rstrip("/").lower()
        return hashlib.sha256(clean.encode()).hexdigest()

    def is_seen(self, url: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM processed_articles WHERE url_hash = ? LIMIT 1",
                (self._hash(url),)
            )
            return cur.fetchone() is not None

    def mark_seen(self, url: str, source_name: str, title: str = ""):
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO processed_articles "
                "(url_hash, url, source_name, title, scraped_at) VALUES (?,?,?,?,?)",
                (
                    self._hash(url),
                    url,
                    source_name,
                    title[:512],
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def total_seen(self) -> int:
        with self._connect() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM processed_articles")
            return cur.fetchone()[0]

    def reset(self):
        with self._connect() as conn:
            conn.execute("DELETE FROM processed_articles")
        logger.info("Deduplication DB cleared.")

# ─────────────────────────── FILTERING ─────────────────────────────────────

def is_idx_relevant(title: str, body: str, keywords: list[str]) -> bool:
    """Return True if title OR body contains at least one IDX-related keyword.
    
    Case-insensitive, whole-word-aware check so 'saham' matches
    'perSAHAMan' too (common in Indonesian).
    """
    combined = (title + " " + body).lower()
    for kw in keywords:
        # word-boundary-aware: match keyword inside words (common in Indonesian morphology)
        if kw.lower() in combined:
            return True
    return False

# ─────────────────────────── HTTP HELPERS ──────────────────────────────────

_ua_index = 0

def _next_user_agent() -> str:
    global _ua_index
    ua = USER_AGENTS[_ua_index % len(USER_AGENTS)]
    _ua_index += 1
    return ua

def fetch_page(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[BeautifulSoup]:
    """Fetch a URL and return a BeautifulSoup object, or None on failure."""
    headers = {
        "User-Agent": _next_user_agent(),
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        # Use lxml for speed; fall back to html.parser if lxml is missing
        try:
            soup = BeautifulSoup(resp.content, "lxml")
        except Exception:
            soup = BeautifulSoup(resp.content, "html.parser")
        return soup
    except requests.exceptions.Timeout:
        logger.warning(f"  Timeout fetching: {url}")
    except requests.exceptions.TooManyRedirects:
        logger.warning(f"  Too many redirects: {url}")
    except requests.exceptions.HTTPError as e:
        logger.warning(f"  HTTP {e.response.status_code} for: {url}")
    except requests.exceptions.ConnectionError:
        logger.warning(f"  Connection error for: {url}")
    except Exception as e:
        logger.warning(f"  Unexpected error for {url}: {e}")
    return None

# ─────────────────────────── EXTRACTION HELPERS ────────────────────────────

def extract_links_from_listing(
    soup: BeautifulSoup,
    source: dict,
    base_url: str,
) -> list[dict]:
    """Extract article title+link pairs from a listing/index page."""
    candidates = []

    for selector in source["article_list_selector"].split(","):
        selector = selector.strip()
        cards = soup.select(selector)
        if cards:
            logger.debug(f"    Matched listing selector '{selector}' → {len(cards)} cards")
            for card in cards[:MAX_ARTICLES_PER_SOURCE]:
                # --- Extract title ---
                title_el = None
                for ts in source["title_selector"].split(","):
                    title_el = card.select_one(ts.strip())
                    if title_el:
                        break
                title = title_el.get_text(strip=True) if title_el else ""

                # --- Extract link ---
                link_el = card.select_one(source["link_selector"].strip())
                href = ""
                if link_el:
                    href = link_el.get("href", "")
                    if not href:
                        href = link_el.get("data-href", "")

                if not href:
                    continue

                # ── 1. Strip all surrounding whitespace and non-printable chars ──
                href = href.strip()

                # ── 2. Resolve relative URLs ─────────────────────────────────────
                prefix = source.get("link_prefix", "")
                if prefix:
                    # Avoid double-slash: strip trailing slash from prefix,
                    # ensure href starts with exactly one slash.
                    href = prefix.rstrip("/") + "/" + href.lstrip("/")
                else:
                    # urljoin handles: absolute URLs (returned unchanged),
                    # protocol-relative (//example.com/path → https://...),
                    # and relative paths (/path or path).
                    href = urljoin(base_url, href)

                # ── 3. Clean up double slashes in the path (not in scheme) ──────
                parsed = urlparse(href)
                clean_path = re.sub(r"/{2,}", "/", parsed.path)
                href = parsed._replace(path=clean_path).geturl()

                # ── 4. Must be a valid http/https URL ────────────────────────────
                parsed = urlparse(href)
                if not parsed.scheme.startswith("http") or not parsed.netloc:
                    logger.debug(f"    Skipping invalid URL: {href!r}")
                    continue

                candidates.append({"title": title, "url": href})

            if candidates:
                break  # stop trying selectors once we have results

    # Deduplicate by URL within this batch
    seen_urls: set[str] = set()
    unique = []
    for c in candidates:
        if c["url"] not in seen_urls:
            seen_urls.add(c["url"])
            unique.append(c)

    return unique


def extract_body_text(soup: BeautifulSoup, body_selectors: list[str]) -> str:
    """Try each CSS selector in order and return the first non-empty body text."""
    for selector in body_selectors:
        container = soup.select_one(selector)
        if container:
            # Remove script/style/nav noise
            for tag in container.select("script, style, nav, .breadcrumb, .related-news, .social-share"):
                tag.decompose()
            text = container.get_text(separator="\n", strip=True)
            # Collapse excessive blank lines
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            if len(text) >= MIN_BODY_LENGTH:
                return text
    return ""

# ─────────────────────────── CORE SCRAPER ──────────────────────────────────

def scrape_source(source: dict, keywords: list[str], db: DeduplicationDB, dry_run: bool) -> list[dict]:
    """Scrape a single source and return a list of relevant, new articles."""
    name         = source["name"]
    listing_url  = source["listing_url"]
    base_url     = source["base_url"]
    delay        = source.get("request_delay_seconds", 2)
    body_sels    = source["body_selectors"]

    logger.info(f"[{name}] Fetching listing page: {listing_url}")
    listing_soup = fetch_page(listing_url)
    if not listing_soup:
        logger.error(f"[{name}] Could not fetch listing page. Skipping.")
        return []

    candidates = extract_links_from_listing(listing_soup, source, base_url)
    logger.info(f"[{name}] Found {len(candidates)} candidate article(s) on listing page")

    results = []

    for idx, cand in enumerate(candidates):
        url   = cand["url"]
        title = cand["title"]

        # ── Deduplication check ──
        if db.is_seen(url):
            logger.debug(f"[{name}] SKIP (already seen): {url}")
            continue

        logger.info(f"[{name}] ({idx+1}/{len(candidates)}) Fetching article: {title[:60]}")
        time.sleep(delay)

        article_soup = fetch_page(url)
        if not article_soup:
            continue

        # Prefer page <h1> as canonical title if listing title was empty
        if not title:
            h1 = article_soup.find("h1")
            title = h1.get_text(strip=True) if h1 else ""

        body = extract_body_text(article_soup, body_sels)

        # ── Relevance filter ──
        if not is_idx_relevant(title, body, keywords):
            logger.debug(f"[{name}] NOT RELEVANT — skipped: {title[:60]}")
            if not dry_run:
                db.mark_seen(url, name, title)  # mark so we don't recheck
            continue

        if len(body) < MIN_BODY_LENGTH:
            logger.debug(f"[{name}] Body too short ({len(body)} chars) — skipped: {url}")
            continue

        article = {
            "source":     name,
            "title":      title,
            "url":        url,
            "body":       body,
            "body_chars": len(body),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

        if not dry_run:
            db.mark_seen(url, name, title)

        results.append(article)
        logger.info(f"[{name}] ✓ SAVED: {title[:70]}")

    logger.info(f"[{name}] Done. {len(results)} new relevant article(s) collected.")
    return results


def load_sources(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_output(articles: list[dict], output_path: str):
    """Merge new articles into raw_news.json (append, not overwrite)."""
    existing = []
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning("raw_news.json was unreadable — starting fresh.")

    combined = existing + articles

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    logger.info(f"raw_news.json updated: {len(articles)} new + {len(existing)} existing = {len(combined)} total articles")

# ─────────────────────────── ENTRY POINT ───────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="IDX News Scraper")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print articles without saving to DB or raw_news.json"
    )
    parser.add_argument(
        "--reset-db", action="store_true",
        help="Clear deduplication history before running"
    )
    parser.add_argument(
        "--source", type=str, default=None,
        help="Scrape only the named source (e.g. 'Kontan')"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("IDX News Scraper — starting run")
    logger.info(f"  Mode   : {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Load configuration
    config   = load_sources(SOURCES_FILE)
    keywords = config.get("keywords", ["saham", "emiten", "bursa"])
    sources  = config.get("sources", [])

    # Initialise deduplication DB
    db = DeduplicationDB(DB_FILE)
    if args.reset_db:
        db.reset()
    logger.info(f"Deduplication DB: {db.total_seen()} articles already processed")

    # Filter sources
    active_sources = [s for s in sources if s.get("enabled", True)]
    if args.source:
        active_sources = [s for s in active_sources if s["name"].lower() == args.source.lower()]
        if not active_sources:
            logger.error(f"No enabled source found with name '{args.source}'. Exiting.")
            return

    logger.info(f"Active sources: {[s['name'] for s in active_sources]}")
    logger.info(f"Filter keywords: {keywords}")

    # Scrape each source
    all_articles: list[dict] = []
    for source in active_sources:
        try:
            articles = scrape_source(source, keywords, db, args.dry_run)
            all_articles.extend(articles)
        except Exception as e:
            logger.error(f"[{source['name']}] Fatal error: {e}", exc_info=True)
        # Inter-source polite pause
        time.sleep(1)

    # Output
    logger.info("=" * 60)
    logger.info(f"Total new articles collected: {len(all_articles)}")

    if args.dry_run:
        logger.info("[DRY RUN] Articles not saved. Preview below:")
        for art in all_articles:
            print(f"\n{'─'*60}")
            print(f"SOURCE : {art['source']}")
            print(f"TITLE  : {art['title']}")
            print(f"URL    : {art['url']}")
            print(f"CHARS  : {art['body_chars']}")
            print(f"BODY   :\n{art['body'][:400]}...")
    else:
        if all_articles:
            save_output(all_articles, OUTPUT_FILE)
        else:
            logger.info("Nothing new to save.")

    logger.info("Run complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
