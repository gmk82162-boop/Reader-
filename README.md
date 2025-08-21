#!/usr/bin/env python3
import csv
import json
import re
import time
import random
import logging
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from urllib import robotparser

BASE = "https://www.bloomberg.com"
HEADERS_LIST = [
    # A small pool of realistic desktop UA strings
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Politeness: 1 request every 2–5 seconds
MIN_DELAY = 2.0
MAX_DELAY = 5.0

# Retry config
MAX_RETRIES = 3
BACKOFF_BASE = 1.5
TIMEOUT = 15

# Simple pattern to keep likely news articles
ARTICLE_PATH_RE = re.compile(r"^/news/articles/[A-Za-z0-9]+", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

@dataclass
class Article:
    url: str
    canonical_url: Optional[str]
    title: Optional[str]
    description: Optional[str]
    published: Optional[str]
    modified: Optional[str]
    section: Optional[str]
    authors: List[str]

def sleep_politely():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def pick_headers() -> Dict[str, str]:
    return {"User-Agent": random.choice(HEADERS_LIST), "Accept-Language": "en-US,en;q=0.9"}

def get_text(meta: Optional[Dict[str, str]]) -> Optional[str]:
    return meta.get("content") if meta else None

def fetch(url: str, session: requests.Session, allow_redirects: bool=True) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=pick_headers(), timeout=TIMEOUT, allow_redirects=allow_redirects)
            if resp.status_code in (200, 301, 302):
                return resp
            elif resp.status_code in (403, 404, 410):
                logging.warning(f"{resp.status_code} for {url}")
                return None
            else:
                logging.warning(f"Status {resp.status_code} for {url}; attempt {attempt}/{MAX_RETRIES}")
        except requests.RequestException as e:
            logging.warning(f"Error fetching {url}: {e}; attempt {attempt}/{MAX_RETRIES}")
        sleep = (BACKOFF_BASE ** (attempt - 1)) + random.random()
        time.sleep(sleep)
    return None

def parse_robots_and_sitemaps(session: requests.Session):
    robots_url = urljoin(BASE, "/robots.txt")
    rp = robotparser.RobotFileParser()
    resp = fetch(robots_url, session)
    if not resp:
        logging.error("Could not fetch robots.txt; aborting for safety.")
        return None, []

    robots_txt = resp.text
    rp.parse(robots_txt.splitlines())

    # Extract Sitemap lines manually so we don’t rely solely on rp.site_maps() (which can be None)
    sitemaps = []
    for line in robots_txt.splitlines():
        if line.lower().startswith("sitemap:"):
            sm_url = line.split(":", 1)[1].strip()
            sitemaps.append(sm_url)

    logging.info(f"Found {len(sitemaps)} sitemap URLs from robots.txt")
    return rp, sitemaps

def sitemap_urls(sitemaps: List[str], session: requests.Session, limit: int = 500) -> Iterable[str]:
    """
    Stream URLs from XML sitemaps. Bloomberg uses multiple sitemaps and indexes.
    We’ll handle simple <urlset> and <sitemapindex>.
    """
    seen = set()
    queue = list(sitemaps)
    count = 0

    while queue and count < limit:
        sm = queue.pop(0)
        resp = fetch(sm, session)
        sleep_politely()
        if not resp:
            continue
        try:
            soup = BeautifulSoup(resp.content, "xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "lxml-xml")

        # If this is an index of sitemaps, enqueue children
        for loc in soup.select("sitemap > loc"):
            child = loc.get_text(strip=True)
            if child and child not in seen:
                seen.add(child)
                queue.append(child)

        # If this is a urlset, yield URLs
        for loc in soup.select("url > loc"):
            url = loc.get_text(strip=True)
            if url and url not in seen:
                seen.add(url)
                yield url
                count += 1
                if count >= limit:
                    break

def is_allowed(url: str, rp: robotparser.RobotFileParser) -> bool:
    path = urlparse(url).path or "/"
    return rp.can_fetch("*", url) and ARTICLE_PATH_RE.match(path) is not None

def extract_article(resp: requests.Response, url: str) -> Article:
    soup = BeautifulSoup(resp.text, "lxml")

    # Prefer structured metadata
    def meta(name=None, prop=None):
        if name:
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.has_attr("content"):
                return {"content": tag["content"]}
        if prop:
            tag = soup.find("meta", attrs={"property": prop})
            if tag and tag.has_attr("content"):
                return {"content": tag["content"]}
        return None

    title = None
    # Common options
    title = get_text(meta(name="og:title")) or get_text(meta(prop="og:title")) or \
            (soup.title.string.strip() if soup.title and soup.title.string else None)

    description = get_text(meta(name="description")) or get_text(meta(prop="og:description"))
    published = get_text(meta(name="article:published_time")) or get_text(meta(prop="article:published_time"))
    modified = get_text(meta(name="article:modified_time")) or get_text(meta(prop="article:modified_time"))
    canonical = None
    link_canon = soup.find("link", rel=lambda v: v and "canonical" in v)
    if link_canon and link_canon.has_attr("href"):
        canonical = link_canon["href"]

    # Section / category
    section = get_text(meta(name="article:section")) or get_text(meta(prop="article:section"))

    # Authors (try JSON-LD first if present)
    authors = set()
    for script in soup.find_all("script", type=lambda v: v and "ld+json" in v):
        try:
            data = json.loads(script.string or "")
            # Normalize to list
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                if obj.get("@type") in ("NewsArticle", "Article", "ReportageNewsArticle"):
                    by = obj.get("author")
                    if isinstance(by, dict) and "name" in by:
                        authors.add(by["name"])
                    elif isinstance(by, list):
                        for a in by:
                            if isinstance(a, dict) and "name" in a:
                                authors.add(a["name"])
        except Exception:
            continue

    # Fallback: byline element patterns
    if not authors:
        byline = soup.find(attrs={"data-testid": re.compile(r"byline", re.I)}) or soup.find(class_=re.compile(r"byline", re.I))
        if byline:
            text = byline.get_text(" ", strip=True)
            # Naive split
            parts = re.split(r"by\s+", text, flags=re.I)
            if parts:
                names = re.split(r",| and ", parts[-1])
                for n in names:
                    n = n.strip()
                    if 2 <= len(n) <= 80 and "©" not in n:
                        authors.add(n)

    return Article(
        url=url,
        canonical_url=canonical,
        title=title,
        description=description,
        published=published,
        modified=modified,
        section=section,
        authors=sorted(authors)
    )

def save_jsonl(path: str, rows: Iterable[Article]):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")

def save_csv(path: str, rows: List[Article]):
    if not rows:
        return
    keys = list(asdict(rows[0]).keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in rows:
            d = asdict(r)
            d["authors"] = ", ".join(r.authors)
            writer.writerow(d)

def main(max_urls: int = 200):
    session = requests.Session()

    rp, sitemaps = parse_robots_and_sitemaps(session)
    if rp is None:
        return
    if not sitemaps:
        logging.error("No sitemaps found in robots.txt; exiting for safety.")
        return

    # Collect article URLs from sitemaps while respecting robots.txt and a simple path pattern
    candidates = []
    for url in sitemap_urls(sitemaps, session, limit=max_urls * 5):  # oversample then filter
        if is_allowed(url, rp):
            candidates.append(url)
        if len(candidates) >= max_urls:
            break

    logging.info(f"Collected {len(candidates)} candidate article URLs")

    articles: List[Article] = []
    for i, url in enumerate(candidates, 1):
        if not rp.can_fetch("*", url):
            logging.info(f"Disallowed by robots.txt: {url}")
            continue

        sleep_politely()
        resp = fetch(url, session)
        if not resp:
            continue
        art = extract_article(resp, url)
        articles.append(art)
        logging.info(f"[{i}/{len(candidates)}] Scraped: {art.title or '(no title)'}")

    # Persist
    save_jsonl("bloomberg_articles.jsonl", articles)
    save_csv("bloomberg_articles.csv", articles)
    logging.info(f"Saved {len(articles)} articles to bloomberg_articles.jsonl and bloomberg_articles.csv")

if __name__ == "__main__":
    # Example: change max_urls as needed
    main(max_urls=120)
