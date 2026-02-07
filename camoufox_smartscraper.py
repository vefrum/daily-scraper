import nest_asyncio
import os
import json
import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

from camoufox.sync_api import Camoufox
from scrapegraphai.graphs import SmartScraperGraph

# Prevent event loop errors in some environments (e.g. notebooks)
nest_asyncio.apply()

# =========================
# CRAWLER CONFIG (edit here)
# =========================
# Choose one:
# - "max_pages": crawl from START_PAGE to MAX_PAGES (good for debugging)
# - "until_empty": keep crawling until a page returns 0 events (best-effort "all pages")
CRAWL_MODE = "max_pages"

START_PAGE = 1
MAX_PAGES = 2  # used only when CRAWL_MODE == "max_pages"

# If CRAWL_MODE == "until_empty", stop after this many pages as a safety cap
SAFETY_MAX_PAGES = 50

BASE_URL = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"

WAIT_SELECTOR = "div.event-list"
TIMEOUT_MS = 10000


def build_url_for_page(base_url: str, page_num: int) -> str:
    """
    Returns base_url with the 'page' query param set to page_num.
    Works even if base_url already has other query params.
    """
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs["page"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def fetch_rendered_html_with_camoufox(
    url: str,
    wait_selector: str = WAIT_SELECTOR,
    timeout_ms: int = TIMEOUT_MS
) -> str:
    """
    Uses Camoufox to load a JS-heavy page and returns the fully rendered HTML.
    """
    with Camoufox(headless=True) as browser:
        page = browser.new_page()

        print(f"Navigating: {url}")
        page.goto(url)

        try:
            page.wait_for_selector(wait_selector, timeout=timeout_ms)
            print("Content loaded!")
        except Exception:
            print("Timed out waiting for selector, grabbing whatever is there...")

        raw_html = page.content()

    return raw_html


def run_smartscraper_on_html(raw_html: str, openai_key: str, today: datetime.date):
    """
    Runs SmartScraperGraph using the provided raw HTML as the source.
    """
    prompt = f"""
        Find all events on the page.
        Extract the following fields: 'title', 'date', 'location', 'price','url'.

        IMPORTANT FORMATTING RULES:
        1. Return ONLY a pure JSON list of objects.
        2. Do NOT wrap the list inside a dictionary (like {{ "events": [...] }}).
        3. Do NOT return the JSON as a string inside another object.
        4. Ensure the output is valid JSON that can be parsed by Python's json.loads().
        5. Convert all relative dates (like 'Tomorrow', 'This Weekend', 'Monday') into strict ISO format (YYYY-MM-DD HH:MM). Today's date is {today}
        """

    config = {
        "llm": {
            "api_key": openai_key,
            "model": "openai/gpt-5-mini"
        },
        "verbose": False,
    }

    scraper = SmartScraperGraph(
        prompt=prompt,
        source=raw_html,
        config=config
    )

    print("Starting extraction... (This might take a minute)")
    result = scraper.run()
    return result


def normalise_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    return url.strip()


def make_fallback_key(event: dict) -> str:
    """
    Fallback dedupe key if url is missing.
    """
    if not isinstance(event, dict):
        return ""
    title = str(event.get("title", "")).strip().lower()
    date = str(event.get("date", "")).strip().lower()
    location = str(event.get("location", "")).strip().lower()
    return f"{title}|{date}|{location}"


def dedupe_events(events: list) -> list:
    """
    Dedupes events primarily by 'url'. If missing/empty, uses title+date+location.
    Keeps the first occurrence.
    """
    seen = set()
    out = []

    for ev in events:
        if not isinstance(ev, dict):
            continue

        url = normalise_url(ev.get("url", ""))
        key = f"url:{url}" if url else f"fallback:{make_fallback_key(ev)}"

        if not key or key in seen:
            continue

        seen.add(key)
        out.append(ev)

    return out


def ensure_list(result):
    """
    SmartScraperGraph sometimes returns a dict or string depending on model behaviour.
    We want a list of dicts.
    """
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return [result]
    return []


def main():
    load_dotenv()
    openai_key = os.getenv("OPENAI_API_KEY")

    if not openai_key:
        print("Error: Could not find API key. Did you create the .env file?")
        raise SystemExit(1)

    today = datetime.date.today()

    all_events = []
    page_num = START_PAGE
    pages_crawled = 0

    while True:
        if CRAWL_MODE == "max_pages" and page_num > MAX_PAGES:
            break
        if CRAWL_MODE == "until_empty" and pages_crawled >= SAFETY_MAX_PAGES:
            print(f"Reached SAFETY_MAX_PAGES={SAFETY_MAX_PAGES}. Stopping.")
            break

        url = build_url_for_page(BASE_URL, page_num)

        raw_html = fetch_rendered_html_with_camoufox(
            url=url,
            wait_selector=WAIT_SELECTOR,
            timeout_ms=TIMEOUT_MS
        )

        try:
            result = run_smartscraper_on_html(raw_html=raw_html, openai_key=openai_key, today=today)
        except Exception as e:
            print(f"Extraction failed on page {page_num}: {e}")
            break

        events = ensure_list(result)
        print(f"Page {page_num}: extracted {len(events)} events")
        all_events.extend(events)

        pages_crawled += 1

        if CRAWL_MODE == "until_empty" and len(events) == 0:
            print("No events found on this page. Stopping.")
            break

        page_num += 1

    all_events = dedupe_events(all_events)
    print(f"Total events after dedupe: {len(all_events)}")

    with open("events.json", "w", encoding="utf-8") as json_file:
        json.dump(all_events, json_file, indent=4, ensure_ascii=False)

    print("Success! Data saved to events.json")


if __name__ == "__main__":
    main()
