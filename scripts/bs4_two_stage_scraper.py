import argparse
import datetime
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import nest_asyncio
import requests
from bs4 import BeautifulSoup

try:
    import dateparser
except Exception:
    dateparser = None

from camoufox.sync_api import Camoufox

nest_asyncio.apply()

# =========================
# CONFIG
# =========================

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

REQUESTS_TIMEOUT_SEC = 25
REQUESTS_RETRIES = 2

# Be polite; also helps reduce blocks.
MIN_DELAY_SEC = 0.3
MAX_DELAY_SEC = 0.9

# Camoufox rendering
CAMOUFOX_TIMEOUT_MS = 20000
SCROLL_PAUSE_SEC = 1.2
MAX_SCROLLS = 2
NO_GROWTH_LIMIT = 3

# Output / cache
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache_html")
DISCOVERY_FILE = os.path.join(DATA_DIR, "events_discovered.json")
ENRICHED_FILE = os.path.join(DATA_DIR, "events_enriched.json")
FAILED_FILE = os.path.join(DATA_DIR, "events_failed.json")

# If True, always use Camoufox for detail pages (slow but sometimes necessary).
FORCE_CAMOUFOX_FOR_DETAILS = False

# =========================
# SOURCES
# =========================
# Note: selectors are best-effort defaults. Expect to tweak per site.
# You can start with Peatix and then refine Eventbrite/Luma/Fever after inspecting HTML.

SOURCES = {
    "peatix": {
        "enabled": True,
        "listing": {
            "strategy": "paged",
            "base_url": "https://peatix.com/search?utm_source=homebanner&p=1",
            "page_param": "p",
            "start_page": 1,
            "max_pages": 3,
            "wait_selector": ".event-card",
            "item_selector": "",
            "html_output_file": os.path.join(DATA_DIR, "rendered_listing_peatix.html"),
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a.event-card__title",  # common
                "a[href*='/event/']",
            ],
            "detail": "peatix",
        },
    },
    "eventbrite": {
        "enabled": False,
        "listing": {
            "strategy": "paged",
            "base_url": "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1",
            "page_param": "page",
            "start_page": 1,
            "max_pages": 3,
            "wait_selector": "body",
            "item_selector": "",
            "html_output_file": os.path.join(DATA_DIR, "rendered_listing_eventbrite.html"),
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='/e/']",
                "a[href*='eventbrite.sg/e/']",
            ],
            "detail": "eventbrite",
        },
    },
    "luma": {
        "enabled": False,
        "listing": {
            "strategy": "infinite_scroll",
            "base_url": "https://luma.com/singapore",
            "page_param": "page",
            "start_page": 1,
            "max_pages": 1,
            "wait_selector": "body",
            "item_selector": ".card-wrapper",
            "html_output_file": os.path.join(DATA_DIR, "rendered_listing_luma.html"),
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='luma.com/']",
                "a[href^='/']",
            ],
            "detail": "luma",
        },
    },
    "fever": {
        "enabled": False,
        "listing": {
            "strategy": "infinite_scroll",
            "base_url": "https://feverup.com/en/singapore/things-to-do",
            "page_param": "page",
            "start_page": 1,
            "max_pages": 1,
            "wait_selector": "body",
            "item_selector": '[data-testid^="fv-plan-card"]',
            "html_output_file": os.path.join(DATA_DIR, "rendered_listing_fever.html"),
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='/en/singapore/']",
                "a[href^='/']",
            ],
            "detail": "fever",
        },
    },
}


# =========================
# UTIL
# =========================

def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)


def polite_delay() -> None:
    time.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalise_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    return url.strip()


def is_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def absolutise_url(href: str, base_url: str) -> str:
    if not href:
        return ""
    return urljoin(base_url, href)


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def build_url_with_page_param(base_url: str, page_param: str, page_num: int) -> str:
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs[page_param] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def cache_path_for_url(url: str, kind: str) -> str:
    # kind: "listing" or "detail"
    return os.path.join(CACHE_DIR, f"{kind}_{sha1(url)}.html")


def read_cached_html(url: str, kind: str) -> Optional[str]:
    path = cache_path_for_url(url, kind)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_cached_html(url: str, kind: str, html: str) -> None:
    path = cache_path_for_url(url, kind)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def strip_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def first_non_empty(*vals: str) -> str:
    for v in vals:
        v = strip_text(v)
        if v:
            return v
    return ""


# =========================
# DATE PARSING
# =========================

def parse_datetime_range_sg(date_text: str, base_dt_sg: datetime.datetime) -> dict:
    """
    Returns:
      {
        "date_text": original,
        "start_datetime_sg": "YYYY-MM-DD HH:MM" or "",
        "end_datetime_sg": "YYYY-MM-DD HH:MM" or ""
      }

    Resolves relative/ambiguous text like "Tomorrow 2pm" or "This weekend".
    If parsing fails, returns empty start/end but keeps date_text.
    """
    date_text = strip_text(date_text)
    out = {
        "date_text": date_text,
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }
    if not date_text:
        return out

    if dateparser is None:
        # No dependency available; keep raw text only.
        return out

    settings = {
        "TIMEZONE": "Asia/Singapore",
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM": "future",
        "RELATIVE_BASE": base_dt_sg,
    }

    # Heuristic split for ranges like "2pm - 4pm" or "2pm to 4pm"
    # This is intentionally simple; per-site parsers can override with better extraction.
    range_match = re.search(r"(.+?)(?:\s+)(?:-|–|to)\s+(.+)", date_text, flags=re.IGNORECASE)
    if range_match:
        left = strip_text(range_match.group(1))
        right = strip_text(range_match.group(2))

        start_dt = dateparser.parse(left, settings=settings)
        end_dt = dateparser.parse(right, settings=settings)

        # If right side is only a time, dateparser might parse it as today; align date with start.
        if start_dt and end_dt and end_dt.date() != start_dt.date():
            # If end_dt has no explicit date, try parsing right with start date context.
            end_dt2 = dateparser.parse(
                f"{start_dt.strftime('%Y-%m-%d')} {right}",
                settings=settings
            )
            if end_dt2:
                end_dt = end_dt2

        if start_dt:
            out["start_datetime_sg"] = start_dt.astimezone(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
        if end_dt:
            out["end_datetime_sg"] = end_dt.astimezone(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
        return out

    # Single datetime
    dt = dateparser.parse(date_text, settings=settings)
    if dt:
        out["start_datetime_sg"] = dt.astimezone(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    return out


# =========================
# CAMOUFOX RENDERING
# =========================

def fetch_rendered_html_with_camoufox(
    url: str,
    wait_selector: str,
    timeout_ms: int,
    scroll_times: int = 0,
    scroll_pause_sec: float = SCROLL_PAUSE_SEC,
    scroll_until_no_growth: bool = False,
    item_selector: str = "",
    no_growth_limit: int = NO_GROWTH_LIMIT,
    headless: bool = True,
) -> str:
    with Camoufox(headless=headless) as browser:
        page = browser.new_page()
        print(f"Camoufox navigating: {url}")
        page.goto(url)

        try:
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
        except Exception:
            print("Camoufox: timed out waiting for selector; continuing...")

        if scroll_until_no_growth and item_selector:
            last_count = -1
            no_growth = 0
            scrolls = 0
            while True:
                try:
                    current_count = page.locator(item_selector).count()
                except Exception:
                    current_count = -1

                if current_count == last_count:
                    no_growth += 1
                else:
                    no_growth = 0

                if no_growth >= no_growth_limit:
                    break
                if scrolls >= MAX_SCROLLS:
                    break

                last_count = current_count
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause_sec)
                scrolls += 1

        elif scroll_times > 0:
            for _ in range(scroll_times):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause_sec)

        return page.content()


# =========================
# REQUESTS FETCH
# =========================

def fetch_html_requests(url: str, session: requests.Session) -> Optional[str]:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    last_err = None
    for attempt in range(REQUESTS_RETRIES + 1):
        try:
            polite_delay()
            resp = session.get(url, headers=headers, timeout=REQUESTS_TIMEOUT_SEC, allow_redirects=True)
            if resp.status_code >= 400:
                last_err = f"HTTP {resp.status_code}"
                continue
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            last_err = str(e)
            continue
    print(f"Requests fetch failed for {url}: {last_err}")
    return None


# =========================
# DISCOVERY (Stage A)
# =========================

def extract_event_urls_from_listing_html(source_name: str, listing_url: str, html: str) -> list[dict]:
    cfg = SOURCES[source_name]
    selectors = cfg["parsers"]["listing_event_link_selectors"]
    soup = BeautifulSoup(html, "html.parser")

    found = []
    seen = set()

    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href") or ""
            url = absolutise_url(href, listing_url)
            url = normalise_url(url)
            if not is_http_url(url):
                continue

            # Basic per-source URL filtering to reduce noise
            if source_name == "peatix" and "/event/" not in url:
                continue
            if source_name == "eventbrite" and "/e/" not in url:
                continue

            if url in seen:
                continue
            seen.add(url)

            title = strip_text(a.get_text(" ", strip=True))
            found.append(
                {
                    "url": url,
                    "title": title,
                    "source": source_name,
                }
            )

    return found


def discover_urls_for_source(source_name: str, use_cache: bool) -> list[dict]:
    cfg = SOURCES[source_name]
    listing = cfg["listing"]
    strategy = listing["strategy"]

    discovered: list[dict] = []

    if strategy == "paged":
        base_url = listing["base_url"]
        page_param = listing["page_param"]
        start_page = int(listing["start_page"])
        max_pages = int(listing["max_pages"])
        wait_selector = listing.get("wait_selector", "body") or "body"

        for page_num in range(start_page, start_page + max_pages):
            url = build_url_with_page_param(base_url, page_param, page_num)

            cached = read_cached_html(url, "listing") if use_cache else None
            if cached is not None:
                html = cached
                print(f"[Stage A] Using cached listing HTML: {url}")
            else:
                html = fetch_rendered_html_with_camoufox(
                    url=url,
                    wait_selector=wait_selector,
                    timeout_ms=CAMOUFOX_TIMEOUT_MS,
                    scroll_times=0,
                )
                write_cached_html(url, "listing", html)

            rows = extract_event_urls_from_listing_html(source_name, url, html)
            print(f"[Stage A] {source_name} page {page_num}: found {len(rows)} event URLs")
            discovered.extend(rows)

    elif strategy == "infinite_scroll":
        url = listing["base_url"]
        wait_selector = listing.get("wait_selector", "body") or "body"
        item_selector = listing.get("item_selector", "") or ""

        cached = read_cached_html(url, "listing") if use_cache else None
        if cached is not None:
            html = cached
            print(f"[Stage A] Using cached listing HTML: {url}")
        else:
            html = fetch_rendered_html_with_camoufox(
                url=url,
                wait_selector=wait_selector,
                timeout_ms=CAMOUFOX_TIMEOUT_MS,
                scroll_times=MAX_SCROLLS if not item_selector else 0,
                scroll_until_no_growth=bool(item_selector),
                item_selector=item_selector,
                no_growth_limit=NO_GROWTH_LIMIT,
            )
            write_cached_html(url, "listing", html)

        rows = extract_event_urls_from_listing_html(source_name, url, html)
        print(f"[Stage A] {source_name}: found {len(rows)} event URLs")
        discovered.extend(rows)

    else:
        raise ValueError(f"Unknown listing strategy: {strategy}")

    # Dedupe by URL
    out = []
    seen = set()
    for r in discovered:
        u = normalise_url(r.get("url", ""))
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(r)
    return out


# =========================
# DETAIL PARSERS (Stage B)
# =========================

def parse_detail_generic(soup: BeautifulSoup) -> dict:
    # Very generic fallback; per-site parsers should override.
    title = ""
    if soup.title and soup.title.string:
        title = strip_text(soup.title.string)

    # Try meta description
    desc = ""
    meta = soup.select_one('meta[name="description"]')
    if meta and meta.get("content"):
        desc = strip_text(meta.get("content"))

    return {
        "title": title,
        "location": "",
        "price": "",
        "capacity": "",
        "description": desc,
        "date_text": "",
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }


def parse_detail_peatix(soup: BeautifulSoup) -> dict:
    # Best-effort selectors; may need tweaking.
    title = first_non_empty(
        soup.select_one("h1") and soup.select_one("h1").get_text(" ", strip=True),
        soup.title.string if soup.title else "",
    )

    # Description often in a content area; try a few.
    desc_node = (
        soup.select_one(".event-description") or
        soup.select_one("[data-testid='event-description']") or
        soup.select_one(".event__description") or
        soup.select_one("article")
    )
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else ""

    # Date/time text
    date_node = soup.select_one("time") or soup.select_one(".event__date") or soup.select_one(".event-date")
    date_text = strip_text(date_node.get_text(" ", strip=True)) if date_node else ""

    # Location
    loc_node = soup.select_one(".event__venue") or soup.select_one(".event-venue") or soup.select_one("[data-testid='venue']")
    location = strip_text(loc_node.get_text(" ", strip=True)) if loc_node else ""

    # Price
    price_node = soup.select_one(".event__ticket") or soup.select_one(".ticket") or soup.select_one("[data-testid='ticket-price']")
    price = strip_text(price_node.get_text(" ", strip=True)) if price_node else ""

    # Capacity / availability
    cap_node = soup.select_one(".event__status") or soup.select_one(".status") or soup.select_one("[data-testid='availability']")
    capacity = strip_text(cap_node.get_text(" ", strip=True)) if cap_node else ""

    return {
        "title": title,
        "location": location,
        "price": price,
        "capacity": capacity,
        "description": description,
        "date_text": date_text,
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }


def parse_detail_eventbrite(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        soup.select_one("h1") and soup.select_one("h1").get_text(" ", strip=True),
        soup.title.string if soup.title else "",
    )

    # Eventbrite description can be in various containers.
    desc_node = (
        soup.select_one("[data-testid='event-description']") or
        soup.select_one(".structured-content") or
        soup.select_one("section[aria-label*='Description']") or
        soup.select_one("article")
    )
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else ""

    # Date/time text
    date_node = (
        soup.select_one("time") or
        soup.select_one("[data-testid='event-date']") or
        soup.select_one("div.event-details__data")
    )
    date_text = strip_text(date_node.get_text(" ", strip=True)) if date_node else ""

    # Location
    loc_node = (
        soup.select_one("[data-testid='event-location']") or
        soup.select_one("div.location-info__address") or
        soup.select_one("section[aria-label*='Location']")
    )
    location = strip_text(loc_node.get_text(" ", strip=True)) if loc_node else ""

    # Price
    price_node = soup.select_one("[data-testid='event-price']") or soup.select_one("div.conversion-bar__panel-info")
    price = strip_text(price_node.get_text(" ", strip=True)) if price_node else ""

    # Capacity/availability
    capacity = ""
    for kw in ("Sold out", "Selling fast", "Few tickets left", "Limited spots"):
        if kw.lower() in soup.get_text(" ", strip=True).lower():
            capacity = kw
            break

    return {
        "title": title,
        "location": location,
        "price": price,
        "capacity": capacity,
        "description": description,
        "date_text": date_text,
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }


def parse_detail_luma(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        soup.select_one("h1") and soup.select_one("h1").get_text(" ", strip=True),
        soup.title.string if soup.title else "",
    )

    desc_node = soup.select_one("main") or soup.select_one("article")
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else ""

    date_text = ""
    time_node = soup.select_one("time")
    if time_node:
        date_text = strip_text(time_node.get_text(" ", strip=True))

    location = ""
    loc_node = soup.select_one("[data-testid='location']") or soup.select_one("a[href*='maps']")
    if loc_node:
        location = strip_text(loc_node.get_text(" ", strip=True))

    return {
        "title": title,
        "location": location,
        "price": "",
        "capacity": "",
        "description": description,
        "date_text": date_text,
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }


def parse_detail_fever(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        soup.select_one("h1") and soup.select_one("h1").get_text(" ", strip=True),
        soup.title.string if soup.title else "",
    )

    desc_node = soup.select_one("main") or soup.select_one("article")
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else ""

    date_text = ""
    time_node = soup.select_one("time")
    if time_node:
        date_text = strip_text(time_node.get_text(" ", strip=True))

    location = ""
    loc_node = soup.select_one("[data-testid='venue']") or soup.select_one("a[href*='maps']")
    if loc_node:
        location = strip_text(loc_node.get_text(" ", strip=True))

    price = ""
    price_node = soup.select_one("[data-testid='price']") or soup.select_one(".price")
    if price_node:
        price = strip_text(price_node.get_text(" ", strip=True))

    return {
        "title": title,
        "location": location,
        "price": price,
        "capacity": "",
        "description": description,
        "date_text": date_text,
        "start_datetime_sg": "",
        "end_datetime_sg": "",
    }


def parse_event_detail(source_name: str, url: str, html: str, base_dt_sg: datetime.datetime) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    parser_key = SOURCES[source_name]["parsers"]["detail"]
    if parser_key == "peatix":
        data = parse_detail_peatix(soup)
    elif parser_key == "eventbrite":
        data = parse_detail_eventbrite(soup)
    elif parser_key == "luma":
        data = parse_detail_luma(soup)
    elif parser_key == "fever":
        data = parse_detail_fever(soup)
    else:
        data = parse_detail_generic(soup)

    # Normalise date text into resolved datetimes (SG)
    dt_info = parse_datetime_range_sg(data.get("date_text", ""), base_dt_sg=base_dt_sg)
    data["date_text"] = dt_info["date_text"]
    data["start_datetime_sg"] = dt_info["start_datetime_sg"]
    data["end_datetime_sg"] = dt_info["end_datetime_sg"]

    # Ensure required fields exist
    data["url"] = url
    data["source"] = source_name

    # If title missing, fallback to discovered title later (handled by caller)
    return data


# =========================
# ENRICHMENT (Stage B)
# =========================

@dataclass
class FetchResult:
    html: Optional[str]
    method: str  # "cache" | "requests" | "camoufox" | "none"


def fetch_detail_html(url: str, source_name: str, session: requests.Session, use_cache: bool) -> FetchResult:
    if use_cache:
        cached = read_cached_html(url, "detail")
        if cached is not None:
            return FetchResult(html=cached, method="cache")

    if not FORCE_CAMOUFOX_FOR_DETAILS:
        html = fetch_html_requests(url, session=session)
        if html:
            # If it looks like a bot block / empty shell, we can still fallback.
            text = strip_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
            if len(text) > 200:
                write_cached_html(url, "detail", html)
                return FetchResult(html=html, method="requests")

    # Fallback to Camoufox
    try:
        html = fetch_rendered_html_with_camoufox(
            url=url,
            wait_selector="body",
            timeout_ms=CAMOUFOX_TIMEOUT_MS,
            scroll_times=0,
        )
        if html:
            write_cached_html(url, "detail", html)
            return FetchResult(html=html, method="camoufox")
    except Exception as e:
        print(f"Camoufox detail fetch failed for {url}: {e}")

    return FetchResult(html=None, method="none")


def dedupe_by_url(rows: list[dict]) -> list[dict]:
    out = []
    seen = set()
    for r in rows:
        u = normalise_url(r.get("url", ""))
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(r)
    return out


def run_stage_a(enabled_sources: list[str], use_cache: bool) -> list[dict]:
    all_rows: list[dict] = []
    for source_name in enabled_sources:
        print(f"\n[Stage A] Discovering URLs for: {source_name}")
        rows = discover_urls_for_source(source_name, use_cache=use_cache)
        all_rows.extend(rows)

    all_rows = dedupe_by_url(all_rows)
    print(f"\n[Stage A] Total discovered unique URLs: {len(all_rows)}")
    return all_rows


def run_stage_b(discovered: list[dict], use_cache: bool, resume: bool) -> tuple[list[dict], list[dict]]:
    session = requests.Session()

    base_dt_sg = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))

    enriched: list[dict] = []
    failed: list[dict] = []

    already_done = {}
    if resume and os.path.exists(ENRICHED_FILE):
        prev = load_json(ENRICHED_FILE, default=[])
        if isinstance(prev, list):
            for ev in prev:
                if isinstance(ev, dict) and ev.get("url"):
                    already_done[ev["url"]] = ev
            enriched = list(already_done.values())
        print(f"[Stage B] Resume enabled. Loaded {len(enriched)} already enriched events.")

    discovered_by_url = {d["url"]: d for d in discovered if isinstance(d, dict) and d.get("url")}
    urls = list(discovered_by_url.keys())

    for idx, url in enumerate(urls, start=1):
        if url in already_done:
            continue

        d = discovered_by_url[url]
        source_name = d.get("source", "")
        if source_name not in SOURCES:
            failed.append({"url": url, "reason": "unknown_source", "source": source_name})
            continue

        print(f"[Stage B] ({idx}/{len(urls)}) Enriching: {url}")
        fr = fetch_detail_html(url, source_name=source_name, session=session, use_cache=use_cache)
        if not fr.html:
            failed.append({"url": url, "reason": "fetch_failed", "source": source_name})
            continue

        try:
            ev = parse_event_detail(source_name, url, fr.html, base_dt_sg=base_dt_sg)
        except Exception as e:
            failed.append({"url": url, "reason": f"parse_failed: {e}", "source": source_name})
            continue

        # Fill missing title from discovery if needed
        if not strip_text(ev.get("title", "")):
            ev["title"] = strip_text(d.get("title", ""))

        ev["fetch_method"] = fr.method

        # If date is still ambiguous/unparsed, you said you want it resolved.
        # We keep date_text always; if parsing failed, start_datetime_sg will be empty.
        # You can decide later whether to drop these or reprocess with better per-site parsing.
        enriched.append(ev)

        # Checkpoint every 50
        if len(enriched) % 50 == 0:
            save_json(ENRICHED_FILE, enriched)
            save_json(FAILED_FILE, failed)
            print(f"[Stage B] Checkpoint saved. Enriched={len(enriched)}, Failed={len(failed)}")

    return enriched, failed


# =========================
# CLI
# =========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Two-stage BS4 scraper: discover URLs then enrich details.")
    p.add_argument("--stage", choices=["a", "b", "ab"], default="ab", help="Which stage(s) to run.")
    p.add_argument("--use-cache", action="store_true", help="Use cached HTML if available.")
    p.add_argument("--no-cache", action="store_true", help="Do not read cache; still writes cache.")
    p.add_argument("--resume", action="store_true", help="Resume Stage B from existing enriched file.")
    p.add_argument("--sources", default="", help="Comma-separated sources to run (default: enabled sources).")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    use_cache = bool(args.use_cache) and not bool(args.no_cache)

    if args.sources.strip():
        enabled_sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    else:
        enabled_sources = [name for name, cfg in SOURCES.items() if cfg.get("enabled")]

    if not enabled_sources:
        print("No sources selected/enabled.")
        raise SystemExit(1)

    discovered: list[dict] = []

    if args.stage in ("a", "ab"):
        discovered = run_stage_a(enabled_sources=enabled_sources, use_cache=use_cache)
        save_json(DISCOVERY_FILE, discovered)
        print(f"[Stage A] Saved discovery file: {DISCOVERY_FILE}")

    if args.stage in ("b", "ab"):
        if not discovered:
            discovered = load_json(DISCOVERY_FILE, default=[])
            if not discovered:
                print("No discovery data found. Run Stage A first or provide discovery file.")
                raise SystemExit(1)

        enriched, failed = run_stage_b(discovered=discovered, use_cache=use_cache, resume=bool(args.resume))
        enriched = dedupe_by_url(enriched)

        save_json(ENRICHED_FILE, enriched)
        save_json(FAILED_FILE, failed)

        print(f"[Stage B] Saved enriched events: {ENRICHED_FILE} (count={len(enriched)})")
        print(f"[Stage B] Saved failed events: {FAILED_FILE} (count={len(failed)})")


if __name__ == "__main__":
    main()
