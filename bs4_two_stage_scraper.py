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
from dotenv import load_dotenv

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
OPENAI_TIMEOUT_SEC = 60

# Be polite; also helps reduce blocks.
MIN_DELAY_SEC = 0.3
MAX_DELAY_SEC = 0.9

# Camoufox rendering
CAMOUFOX_TIMEOUT_MS = 20000
SCROLL_PAUSE_SEC = 1.2
MAX_SCROLLS = 2
NO_GROWTH_LIMIT = 3

# Global default for paged sources (Stage A)
DEFAULT_MAX_PAGES = 2

# Output / cache
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache_html")
DISCOVERY_FILE = os.path.join(DATA_DIR, "events_discovered.json")
ENRICHED_FILE = os.path.join(DATA_DIR, "events_enriched.json")
FAILED_FILE = os.path.join(DATA_DIR, "events_failed.json")
FUN_EVENTS_FILE = os.path.join(DATA_DIR, "events_fun.json")
REMOVED_EVENTS_FILE = os.path.join(DATA_DIR, "events_removed.json")

# If True, always use Camoufox for detail pages (slow but sometimes necessary).
FORCE_CAMOUFOX_FOR_DETAILS = False

# Debugging / inspection
SAVE_HTML = False
HTML_DUMP_DIR = os.path.join(DATA_DIR, "html_dumps")

# Vibe filtering (Stage C)
VIBE_BATCH_SIZE = 30
VIBE_TAXONOMY = [
    "nightlife_party",
    "live_music_gig",
    "comedy_improv",
    "food_drink",
    "sports_fitness",
    "workshop_fun_crafts",
    "workshop_upskilling",
    "arts_culture",
    "social_mixer",
    "business_networking",
    "outdoor_adventure",
    "festival_market",
    "family_kids",
    "religious_spiritual",
    "corporate_professional",
    "other",
]
REMOVED_CATEGORIES = {
    "business_networking",
    "corporate_professional",
    "workshop_upskilling",
    "religious_spiritual",
    "family_kids",
    "other",
}

# =========================
# SOURCES
# =========================
# Note: selectors are best-effort defaults. Expect to tweak per site.

SOURCES = {
    "peatix": {
        "enabled": True,
        "listing": {
            "strategy": "paged",
            "base_url": "https://peatix.com/search?utm_source=homebanner&p=1",
            "page_param": "p",
            "start_page": 1,
            "wait_selector": ".event-card",
            "item_selector": "",
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a.event-card__title",
                "a[href*='/event/']",
            ],
            "listing_title_selectors": [
                "h3.event-name",
                "h3.promoted-event-name",
                ".event-card__title",
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
            "wait_selector": "body",
            "item_selector": "",
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='/e/']",
                "a[href*='eventbrite.sg/e/']",
            ],
            "listing_title_selectors": [
                "h1",
                "h2",
                "h3",
                "[data-testid='event-card-title']",
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
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='luma.com/']",
                "a[href^='/']",
            ],
            "listing_title_selectors": [
                "h1",
                "h2",
                "h3",
                "[data-testid='event-name']",
            ],
            "detail": "luma",
        },
    },
    "fever": {
        "enabled": False,
        "listing": {
            "strategy": "infinite_scroll",
            "base_url": "https://feverup.com/en/singapore/things-to-do?_gl=1*175v6iz*_up*MQ..*_ga*Njk4MTYyMjc4LjE3NzA0NjM4NDk.*_ga_L4M4ND4NG4*czE3NzA0NjM4NDUkbzEkZzAkdDE3NzA0NjM4NDUkajYwJGwwJGg3MzQzOTc4NDc.*_ga_D4T4V3RS3*czE3NzA0NjM4NDgkbzEkZzAkdDE3NzA0NjM4NDgkajYwJGwwJGgw",
            "page_param": "page",
            "start_page": 1,
            "max_pages": 1,
            "wait_selector": "body",
            "item_selector": '[data-testid^="fv-plan-card"]',
        },
        "parsers": {
            "listing_event_link_selectors": [
                "a[href*='/en/singapore/']",
                "a[href^='/']",
            ],
            "listing_title_selectors": [
                "h1",
                "h2",
                "h3",
                "[data-testid='plan-title']",
            ],
            "detail": "fever",
        },
    },
}


# =========================
# UTIL
# =========================

SG_TZ = datetime.timezone(datetime.timedelta(hours=8))


def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(HTML_DUMP_DIR, exist_ok=True)


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


def dump_html(filename: str, html: str) -> None:
    path = os.path.join(HTML_DUMP_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[Debug] Saved HTML dump: {path} ({len(html)} chars)")


def strip_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def first_non_empty(*vals: str) -> str:
    for v in vals:
        v = strip_text(v)
        if v:
            return v
    return ""


def empty_event(source: str, url: str) -> dict:
    return {
        "source": source,
        "url": url,
        "title": "",
        "description": "",
        "location": "",
        "price": "",
        "capacity": "",
        "date_text": "",
        "start_datetime_sg": "",
    }


def merge_event(base: dict, patch: dict) -> dict:
    """
    Merge patch into base, but only fill fields that are empty in base.
    """
    out = dict(base)
    for k, v in (patch or {}).items():
        if k not in out:
            out[k] = v
            continue
        if strip_text(str(out.get(k, ""))) == "" and strip_text(str(v)) != "":
            out[k] = v
    return out


def meta_name(soup: BeautifulSoup, name: str) -> str:
    node = soup.select_one(f'meta[name="{name}"]')
    if node and node.get("content"):
        return strip_text(node.get("content"))
    return ""


def meta_property(soup: BeautifulSoup, prop: str) -> str:
    node = soup.select_one(f'meta[property="{prop}"]')
    if node and node.get("content"):
        return strip_text(node.get("content"))
    return ""


def select_text(soup: BeautifulSoup, css: str) -> str:
    node = soup.select_one(css)
    if not node:
        return ""
    return strip_text(node.get_text(" ", strip=True))


def to_iso_sg(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SG_TZ)
    return dt.astimezone(SG_TZ).isoformat(timespec="minutes")


def parse_iso_like_to_iso_sg(s: str) -> str:
    """
    Accepts strings like:
      - 2026-03-15T10:00
      - 2026-03-15T10:00:00
      - 2026-03-15 10:00
      - 2026-03-15T10:00+08:00
    Returns ISO 8601 with +08:00, minutes precision, or "" if cannot parse.
    """
    s = strip_text(s)
    if not s:
        return ""

    # Normalise space to T
    s2 = s.replace(" ", "T")

    # If no timezone info, assume SG
    has_tz = bool(re.search(r"(Z|[+-]\d{2}:\d{2})$", s2))
    try:
        dt = datetime.datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SG_TZ)
        return dt.astimezone(SG_TZ).isoformat(timespec="minutes")
    except Exception:
        pass

    if not has_tz:
        # Try parsing without seconds
        try:
            dt = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M")
            dt = dt.replace(tzinfo=SG_TZ)
            return to_iso_sg(dt)
        except Exception:
            pass
        try:
            dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M")
            dt = dt.replace(tzinfo=SG_TZ)
            return to_iso_sg(dt)
        except Exception:
            pass

    return ""


# =========================
# DATE PARSING
# =========================

def parse_datetime_sg_to_iso(date_text: str, base_dt_sg: datetime.datetime) -> dict:
    """
    Returns:
      {
        "date_text": original,
        "start_datetime_sg": ISO 8601 with +08:00 or ""
      }
    """
    date_text = strip_text(date_text)
    out = {
        "date_text": date_text,
        "start_datetime_sg": "",
    }
    if not date_text:
        return out

    if dateparser is None:
        return out

    settings = {
        "TIMEZONE": "Asia/Singapore",
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM": "future",
        "RELATIVE_BASE": base_dt_sg,
    }

    dt = dateparser.parse(date_text, settings=settings)
    if dt:
        out["start_datetime_sg"] = to_iso_sg(dt)
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
    for _attempt in range(REQUESTS_RETRIES + 1):
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

def extract_listing_title_from_anchor(a_tag, title_selectors: list[str]) -> str:
    for sel in title_selectors or []:
        try:
            node = a_tag.select_one(sel)
        except Exception:
            node = None
        if node:
            t = strip_text(node.get_text(" ", strip=True))
            if t:
                return t

    for attr in ("title", "aria-label"):
        t = strip_text(a_tag.get(attr, ""))
        if t:
            return t

    return strip_text(a_tag.get_text(" ", strip=True))


def extract_event_urls_from_listing_html(source_name: str, listing_url: str, html: str) -> list[dict]:
    cfg = SOURCES[source_name]
    selectors = cfg["parsers"]["listing_event_link_selectors"]
    title_selectors = cfg["parsers"].get("listing_title_selectors", [])
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

            title = extract_listing_title_from_anchor(a, title_selectors=title_selectors)
            found.append(
                {
                    "url": url,
                    "title": title,
                    "source": source_name,
                }
            )

    return found


def discover_urls_for_source(source_name: str, use_cache: bool, max_pages_override: Optional[int]) -> list[dict]:
    cfg = SOURCES[source_name]
    listing = cfg["listing"]
    strategy = listing["strategy"]

    discovered: list[dict] = []

    if strategy == "paged":
        base_url = listing["base_url"]
        page_param = listing["page_param"]
        start_page = int(listing.get("start_page", 1))
        max_pages = int(listing.get("max_pages", DEFAULT_MAX_PAGES))
        if max_pages_override is not None:
            max_pages = int(max_pages_override)

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

            if SAVE_HTML:
                dump_html(f"listing_{source_name}_page_{page_num}.html", html)

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

        if SAVE_HTML:
            dump_html(f"listing_{source_name}.html", html)

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
    title = ""
    if soup.title and soup.title.string:
        title = strip_text(soup.title.string)

    desc = meta_name(soup, "description")

    return {
        "title": title,
        "location": "",
        "price": "",
        "capacity": "",
        "description": desc,
        "date_text": "",
        "start_datetime_sg": "",
    }


def _parse_peatix_schema_org_event(soup: BeautifulSoup) -> dict:
    """
    Peatix: schema.org microdata is usually present even when app content isn't rendered.
    We extract:
      - title: meta[itemprop=name]@content
      - start_datetime_sg: meta[itemprop=startDate]@content -> normalised ISO +08:00
      - location: venue + address (Place name + address)
      - price: raw number only (Offer price)
    """
    out = {
        "title": "",
        "start_datetime_sg": "",
        "location": "",
        "price": "",
    }

    event_scope = soup.select_one('[itemscope][itemtype="http://schema.org/Event"]')
    if not event_scope:
        return out

    def meta_content(scope, prop: str) -> str:
        if not scope:
            return ""
        node = scope.select_one(f'meta[itemprop="{prop}"]')
        if node and node.get("content"):
            return strip_text(node.get("content"))
        return ""

    title = meta_content(event_scope, "name")
    start_raw = meta_content(event_scope, "startDate")
    start_iso = parse_iso_like_to_iso_sg(start_raw)

    loc_scope = event_scope.select_one('[itemprop="location"][itemscope]')
    venue = meta_content(loc_scope, "name")
    address = meta_content(loc_scope, "address")
    location = strip_text(", ".join([p for p in [venue, address] if strip_text(p)]))

    offer_scope = event_scope.select_one('[itemprop="offers"][itemscope]')
    price = meta_content(offer_scope, "price")

    out.update(
        {
            "title": title,
            "start_datetime_sg": start_iso,
            "location": location,
            "price": price,
        }
    )
    return out


def parse_detail_peatix(soup: BeautifulSoup) -> dict:
    # Layer 1: schema.org microdata
    schema_patch = _parse_peatix_schema_org_event(soup)

    # Layer 2: meta tags
    meta_patch = {
        "title": first_non_empty(meta_property(soup, "og:title"), meta_name(soup, "title")),
        "description": meta_name(soup, "description"),
    }

    # Layer 3: visible HTML fallback (may be empty if app not rendered)
    visible_patch = {
        "title": first_non_empty(select_text(soup, "h1"), strip_text(soup.title.string if soup.title else "")),
        "description": strip_text(
            (
                (soup.select_one(".event-description") or
                 soup.select_one("[data-testid='event-description']") or
                 soup.select_one(".event__description") or
                 soup.select_one("article"))
                .get_text("\n", strip=True)
            ) if (soup.select_one(".event-description") or
                  soup.select_one("[data-testid='event-description']") or
                  soup.select_one(".event__description") or
                  soup.select_one("article")) else ""
        ),
        "location": first_non_empty(
            select_text(soup, ".event__venue"),
            select_text(soup, ".event-venue"),
            select_text(soup, "[data-testid='venue']"),
        ),
        "price": first_non_empty(
            select_text(soup, ".event__ticket"),
            select_text(soup, ".ticket"),
            select_text(soup, "[data-testid='ticket-price']"),
        ),
    }

    ev = empty_event(source="peatix", url="")
    ev = merge_event(ev, schema_patch)
    ev = merge_event(ev, meta_patch)
    ev = merge_event(ev, visible_patch)

    # Peatix capacity/date_text are not reliably present in non-rendered HTML; keep empty by default.
    ev["capacity"] = ""
    ev["date_text"] = ""

    # Ensure price is raw number only (if visible_patch filled it with text like "SGD 25", try to extract number)
    if ev.get("price"):
        m = re.search(r"(\d+(?:\.\d+)?)", ev["price"])
        if m:
            ev["price"] = m.group(1)

    return ev


def parse_detail_eventbrite(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        select_text(soup, "h1"),
        strip_text(soup.title.string if soup.title else ""),
        meta_property(soup, "og:title"),
    )

    desc_node = (
        soup.select_one("[data-testid='event-description']") or
        soup.select_one(".structured-content") or
        soup.select_one("section[aria-label*='Description']") or
        soup.select_one("article")
    )
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else meta_name(soup, "description")

    date_node = (
        soup.select_one("time") or
        soup.select_one("[data-testid='event-date']") or
        soup.select_one("div.event-details__data")
    )
    date_text = strip_text(date_node.get_text(" ", strip=True)) if date_node else ""

    loc_node = (
        soup.select_one("[data-testid='event-location']") or
        soup.select_one("div.location-info__address") or
        soup.select_one("section[aria-label*='Location']")
    )
    location = strip_text(loc_node.get_text(" ", strip=True)) if loc_node else ""

    price_node = soup.select_one("[data-testid='event-price']") or soup.select_one("div.conversion-bar__panel-info")
    price = strip_text(price_node.get_text(" ", strip=True)) if price_node else ""

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
    }


def parse_detail_luma(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        select_text(soup, "h1"),
        strip_text(soup.title.string if soup.title else ""),
        meta_property(soup, "og:title"),
    )

    desc_node = soup.select_one("main") or soup.select_one("article")
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else meta_name(soup, "description")

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
    }


def parse_detail_fever(soup: BeautifulSoup) -> dict:
    title = first_non_empty(
        select_text(soup, "h1"),
        strip_text(soup.title.string if soup.title else ""),
        meta_property(soup, "og:title"),
    )

    desc_node = soup.select_one("main") or soup.select_one("article")
    description = strip_text(desc_node.get_text("\n", strip=True)) if desc_node else meta_name(soup, "description")

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

    # Standardise schema
    ev = empty_event(source=source_name, url=url)
    ev = merge_event(ev, data)

    # Normalise start_datetime_sg if it isn't already ISO
    if ev.get("start_datetime_sg"):
        ev["start_datetime_sg"] = parse_iso_like_to_iso_sg(ev["start_datetime_sg"]) or ev["start_datetime_sg"]

    # If no start_datetime_sg but have date_text, parse it
    if not strip_text(ev.get("start_datetime_sg", "")) and strip_text(ev.get("date_text", "")):
        dt_info = parse_datetime_sg_to_iso(ev.get("date_text", ""), base_dt_sg=base_dt_sg)
        ev["date_text"] = dt_info["date_text"]
        ev["start_datetime_sg"] = dt_info["start_datetime_sg"]

    return ev


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
            text = strip_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
            if len(text) > 200:
                write_cached_html(url, "detail", html)
                return FetchResult(html=html, method="requests")

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


def run_stage_a(enabled_sources: list[str], use_cache: bool, max_pages_override: Optional[int]) -> list[dict]:
    all_rows: list[dict] = []
    for source_name in enabled_sources:
        print(f"\n[Stage A] Discovering URLs for: {source_name}")
        rows = discover_urls_for_source(source_name, use_cache=use_cache, max_pages_override=max_pages_override)
        all_rows.extend(rows)

    all_rows = dedupe_by_url(all_rows)
    print(f"\n[Stage A] Total discovered unique URLs: {len(all_rows)}")
    return all_rows


def run_stage_b(discovered: list[dict], use_cache: bool, resume: bool) -> tuple[list[dict], list[dict]]:
    session = requests.Session()
    base_dt_sg = datetime.datetime.now(SG_TZ)

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

        if SAVE_HTML:
            dump_html(f"detail_{source_name}_{sha1(url)}.html", fr.html)

        try:
            ev = parse_event_detail(source_name, url, fr.html, base_dt_sg=base_dt_sg)
        except Exception as e:
            failed.append({"url": url, "reason": f"parse_failed: {e}", "source": source_name})
            continue

        if not strip_text(ev.get("title", "")):
            ev["title"] = strip_text(d.get("title", ""))

        ev["fetch_method"] = fr.method
        enriched.append(ev)

        if len(enriched) % 50 == 0:
            save_json(ENRICHED_FILE, enriched)
            save_json(FAILED_FILE, failed)
            print(f"[Stage B] Checkpoint saved. Enriched={len(enriched)}, Failed={len(failed)}")

    return enriched, failed


# =========================
# VIBE FILTERING (Stage C)
# =========================

def chunk_list(items: list, chunk_size: int) -> list[list]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def build_event_id(event: dict) -> str:
    url = normalise_url(str(event.get("url", "")))
    if url:
        return f"url:{url}"
    basis = f"{strip_text(str(event.get('title', '')))}|{strip_text(str(event.get('description', '')))}"
    return f"fallback:{sha1(basis)}"


def _simple_vibe_heuristic(event: dict) -> str:
    text = f"{str(event.get('title', ''))} {str(event.get('description', ''))}".strip().lower()
    if not text:
        return "other"

    if any(k in text for k in ["networking", "conference", "summit", "seminar", "webinar", "panel", "b2b"]):
        return "business_networking"
    if any(k in text for k in ["corporate", "leadership", "executive", "enterprise", "professional"]):
        return "corporate_professional"
    if any(k in text for k in ["course", "bootcamp", "cert", "certification", "excel", "python", "sql", "training"]):
        return "workshop_upskilling"
    if any(k in text for k in ["church", "temple", "prayer", "worship", "meditation"]):
        return "religious_spiritual"
    if any(k in text for k in ["kids", "children", "family-friendly", "family friendly", "toddler"]):
        return "family_kids"
    if any(k in text for k in ["party", "rave", "club", "dj", "nightlife"]):
        return "nightlife_party"
    if any(k in text for k in ["concert", "live music", "gig", "band", "jazz"]):
        return "live_music_gig"
    if any(k in text for k in ["comedy", "stand-up", "stand up", "improv"]):
        return "comedy_improv"
    if any(k in text for k in ["brunch", "tasting", "wine", "cocktail", "beer", "dinner", "food festival"]):
        return "food_drink"
    if any(k in text for k in ["yoga", "fitness", "run", "running", "workout", "pilates"]):
        return "sports_fitness"
    if any(k in text for k in ["pottery", "candle", "art jam", "painting", "craft", "diy", "floral", "mixology"]):
        return "workshop_fun_crafts"
    if any(k in text for k in ["hike", "hiking", "trail", "kayak", "cycling", "climb", "adventure"]):
        return "outdoor_adventure"
    if any(k in text for k in ["festival", "market", "bazaar", "fair"]):
        return "festival_market"
    if any(k in text for k in ["museum", "exhibition", "theatre", "theater", "culture", "poetry"]):
        return "arts_culture"
    if any(k in text for k in ["social", "mixer", "meetup", "meet-up", "dating"]):
        return "social_mixer"
    return "other"


def _extract_first_json_array(text: str) -> list:
    text = text or ""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        return []
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def classify_event_vibes_batched(events: list[dict], openai_key: str, vibe_model: str) -> dict[str, str]:
    payload = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        event_id = build_event_id(ev)
        title = strip_text(str(ev.get("title", "")))
        description = strip_text(str(ev.get("description", "")))
        if not event_id or not title:
            continue
        payload.append(
            {
                "id": event_id,
                "title": title,
                "description": description,
            }
        )

    if not payload:
        return {}

    taxonomy_str = ", ".join([f'"{x}"' for x in VIBE_TAXONOMY])
    prompt = f"""
Classify each event into exactly one category for a "fun events" feed.

Allowed categories:
[{taxonomy_str}]

Use only title and description. Choose the best match. Use "other" only if none fits.

Return ONLY JSON array:
[
  {{"id":"...", "category":"one_allowed_category"}},
  ...
]

Events:
{json.dumps(payload, ensure_ascii=False)}
"""

    headers = {
        "Authorization": f"Bearer {openai_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": vibe_model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "You are a strict event classifier. Output only valid JSON."},
            {"role": "user", "content": prompt},
        ],
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=body,
        timeout=OPENAI_TIMEOUT_SEC,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenAI API error {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    content = ""
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        content = ""

    if isinstance(content, list):
        parts = []
        for c in content:
            if isinstance(c, dict) and c.get("type") == "text":
                parts.append(str(c.get("text", "")))
        content = "\n".join(parts)
    elif not isinstance(content, str):
        content = str(content or "")

    rows = _extract_first_json_array(content)
    mapping: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        event_id = strip_text(str(row.get("id", "")))
        category = strip_text(str(row.get("category", "")))
        if not event_id or category not in VIBE_TAXONOMY:
            continue
        mapping[event_id] = category
    return mapping


def apply_vibe_filtering(events: list[dict], openai_key: str, vibe_model: str, batch_size: int) -> tuple[list[dict], list[dict]]:
    kept: list[dict] = []
    removed: list[dict] = []

    id_to_category: dict[str, str] = {}
    batches = chunk_list(events, batch_size)
    for idx, batch in enumerate(batches, start=1):
        print(f"[Stage C] Classifying batch {idx}/{len(batches)} (size={len(batch)})")
        batch_map: dict[str, str] = {}
        try:
            batch_map = classify_event_vibes_batched(batch, openai_key=openai_key, vibe_model=vibe_model)
        except Exception as e:
            print(f"[Stage C] LLM classification failed for batch {idx}: {e}")

        for ev in batch:
            if not isinstance(ev, dict):
                continue
            event_id = build_event_id(ev)
            if event_id not in batch_map:
                batch_map[event_id] = _simple_vibe_heuristic(ev)

        id_to_category.update(batch_map)
        time.sleep(0.2)

    for ev in events:
        if not isinstance(ev, dict):
            continue
        event_id = build_event_id(ev)
        category = id_to_category.get(event_id, _simple_vibe_heuristic(ev))
        ev["vibe_category"] = category
        if category in REMOVED_CATEGORIES:
            removed.append(ev)
        else:
            kept.append(ev)

    return kept, removed


# =========================
# CLI
# =========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-stage BS4 scraper: discover URLs, enrich details, then vibe-filter.")
    p.add_argument("--stage", choices=["a", "b", "c", "ab", "ac", "bc", "abc"], default="ab", help="Which stage(s) to run.")
    p.add_argument("--use-cache", action="store_true", help="Use cached HTML if available.")
    p.add_argument("--no-cache", action="store_true", help="Do not read cache; still writes cache.")
    p.add_argument("--resume", action="store_true", help="Resume Stage B from existing enriched file.")
    p.add_argument("--sources", default="", help="Comma-separated sources to run (default: enabled sources).")
    p.add_argument("--max-pages", type=int, default=None, help="Override max pages for paged sources in Stage A.")
    p.add_argument("--save-html", action="store_true", help="Dump listing/detail HTML into data/html_dumps for selector tuning.")
    p.add_argument("--vibe-model", default="gpt-5-mini", help="Model for Stage C vibe classification.")
    p.add_argument("--vibe-batch-size", type=int, default=VIBE_BATCH_SIZE, help="Batch size for Stage C classification.")
    return p.parse_args()


def main() -> None:
    global SAVE_HTML

    ensure_dirs()
    load_dotenv()
    args = parse_args()
    selected_stages = set(args.stage)

    SAVE_HTML = bool(args.save_html)

    use_cache = bool(args.use_cache) and not bool(args.no_cache)

    if args.sources.strip():
        enabled_sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    else:
        enabled_sources = [name for name, cfg in SOURCES.items() if cfg.get("enabled")]

    if ("a" in selected_stages or "b" in selected_stages) and not enabled_sources:
        print("No sources selected/enabled.")
        raise SystemExit(1)

    discovered: list[dict] = []

    if "a" in selected_stages:
        discovered = run_stage_a(
            enabled_sources=enabled_sources,
            use_cache=use_cache,
            max_pages_override=args.max_pages,
        )
        save_json(DISCOVERY_FILE, discovered)
        print(f"[Stage A] Saved discovery file: {DISCOVERY_FILE}")

    enriched: list[dict] = []

    if "b" in selected_stages:
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

    if "c" in selected_stages:
        if not enriched:
            enriched = load_json(ENRICHED_FILE, default=[])
            if not enriched:
                print("No enriched data found. Run Stage B first or provide enriched file.")
                raise SystemExit(1)

        openai_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not openai_key:
            print("OPENAI_API_KEY not found. Add it in environment or .env for Stage C.")
            raise SystemExit(1)

        batch_size = max(1, int(args.vibe_batch_size))
        kept, removed = apply_vibe_filtering(
            enriched,
            openai_key=openai_key,
            vibe_model=args.vibe_model,
            batch_size=batch_size,
        )

        save_json(FUN_EVENTS_FILE, kept)
        save_json(REMOVED_EVENTS_FILE, removed)
        print(f"[Stage C] Saved fun events: {FUN_EVENTS_FILE} (count={len(kept)})")
        print(f"[Stage C] Saved removed events: {REMOVED_EVENTS_FILE} (count={len(removed)})")


if __name__ == "__main__":
    main()
