import nest_asyncio
import os
import json
import datetime
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

from camoufox.sync_api import Camoufox
from scrapegraphai.graphs import SmartScraperGraph

# Prevent event loop errors in some environments (e.g. notebooks)
nest_asyncio.apply()

# =========================
# CRAWLER CONFIG (edit here)
# =========================

# Quick test run: keep this low first to validate extraction works across sources.
MAX_SCROLLS = 2
SCROLL_PAUSE_SEC = 1.2
NO_GROWTH_LIMIT = 3  # stop after N consecutive scrolls with no increase in item count

# Debugging / inspection
SAVE_HTML = False

# =========================
# SOURCES (paste your URLs here)
# =========================
# For each source:
# - enabled: set True/False
# - base_url: listing URL
# - crawl_strategy: "paged" or "infinite_scroll"
# - page_param: only for "paged" (e.g. "page" for Eventbrite, "p" for Peatix)
# - start_page/max_pages/stop_mode/safety_max_pages: only for "paged"
# - wait_selector: selector to wait for initial content
# - item_selector: only for "infinite_scroll" adaptive stop (optional; leave "" to use fixed MAX_SCROLLS)
# - html_output_file: where to save HTML if SAVE_HTML=True (per source)
SOURCES = {
    "eventbrite": {
        "enabled": True,
        "base_url": "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1",
        "crawl_strategy": "paged",
        "page_param": "page",
        "start_page": 1,
        "max_pages": 2,
        "stop_mode": "max_pages",  # "max_pages" or "until_empty"
        "safety_max_pages": 50,
        "wait_selector": "div.event-list",
        "item_selector": "",
        "html_output_file": "scraped_eventbrite.html",
    },
    # Paste your other sources here, example:
    "luma": {
        "enabled": True,
        "base_url": "https://luma.com/singapore",
        "crawl_strategy": "infinite_scroll",
        "page_param": "page",
        "start_page": 1,
        "max_pages": 1,
        "stop_mode": "max_pages",
        "safety_max_pages": 50,
        "wait_selector": "body",
        "item_selector": ".card-wrapper",
        "html_output_file": "scraped_luma.html",
    },
    "fever": {
        "enabled": True,
        "base_url": "https://feverup.com/en/singapore/things-to-do?_gl=1*175v6iz*_up*MQ..*_ga*Njk4MTYyMjc4LjE3NzA0NjM4NDk.*_ga_L4M4ND4NG4*czE3NzA0NjM4NDUkbzEkZzAkdDE3NzA0NjM4NDUkajYwJGwwJGg3MzQzOTc4NDc.*_ga_D4T4V3RS3*czE3NzA0NjM4NDgkbzEkZzAkdDE3NzA0NjM4NDgkajYwJGwwJGgw",
        "crawl_strategy": "infinite_scroll",
        "page_param": "page",
        "start_page": 1,
        "max_pages": 1,
        "stop_mode": "max_pages",
        "safety_max_pages": 50,
        "wait_selector": "body",
        "item_selector": '[data-testid^="fv-plan-card"]',
        "html_output_file": "scraped_fever.html",
    },
    "peatix": {
        "enabled": True,
        "base_url": "https://peatix.com/search?utm_source=homebanner&p=1",
        "crawl_strategy": "paged",
        "page_param": "p",
        "start_page": 1,
        "max_pages": 2,
        "stop_mode": "max_pages",
        "safety_max_pages": 50,
        "wait_selector": "body",
        "item_selector": "",
        "html_output_file": "scraped_peatix.html",
    },
}


def build_url_with_page_param(base_url: str, page_param: str, page_num: int) -> str:
    """
    Returns base_url with the given page_param set to page_num.
    Works even if base_url already has other query params.
    Example: page_param="page" (Eventbrite), page_param="p" (Peatix).
    """
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs[page_param] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def save_html(html: str, filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved rendered HTML to {filename} ({len(html)} chars)")


def fetch_rendered_html_with_camoufox(
    url: str,
    wait_selector: str,
    timeout_ms: int,
    scroll_times: int = 0,
    scroll_pause_sec: float = SCROLL_PAUSE_SEC,
    scroll_until_no_growth: bool = False,
    item_selector: str = "",
    no_growth_limit: int = NO_GROWTH_LIMIT,
) -> str:
    """
    Uses Camoufox to load a JS-heavy page and returns the fully rendered HTML.

    Scrolling options:
    - Fixed scrolling: set scroll_times > 0
    - Adaptive scrolling: set scroll_until_no_growth=True and provide item_selector
      It will stop when the number of matched elements stops increasing for no_growth_limit rounds.
    """
    with Camoufox(headless=True) as browser:
        page = browser.new_page()

        print(f"Navigating: {url}")
        page.goto(url)

        try:
            page.wait_for_selector(wait_selector, timeout=timeout_ms)
            print("Content loaded!")
        except Exception:
            print("Timed out waiting for selector, continuing anyway...")

        if scroll_until_no_growth and item_selector:
            print(
                f"Scrolling until no growth using ITEM_SELECTOR='{item_selector}' "
                f"(no_growth_limit={no_growth_limit}, max_scrolls={MAX_SCROLLS})..."
            )
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
                    print(f"No growth for {no_growth_limit} rounds. Stopping scroll.")
                    break

                if scrolls >= MAX_SCROLLS:
                    print(f"Reached MAX_SCROLLS={MAX_SCROLLS}. Stopping scroll.")
                    break

                last_count = current_count
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause_sec)
                scrolls += 1

                if scrolls % 10 == 0:
                    print(f"Scrolled {scrolls} times; current item count={current_count}")

        elif scroll_times > 0:
            print(f"Scrolling {scroll_times} times to load more content...")
            for i in range(scroll_times):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause_sec)
                if (i + 1) % 10 == 0:
                    print(f"Scrolled {i + 1}/{scroll_times}")

        raw_html = page.content()

    return raw_html


def run_smartscraper_on_html(raw_html: str, openai_key: str, today: datetime.date):
    """
    Runs SmartScraperGraph using the provided raw HTML as the source.
    """
    prompt = f"""
        Find all events on the page.
        Extract the following fields: 'title', 'date', 'location', 'price', 'capacity', 'url'.

        Field notes:
        - 'capacity' should capture availability signals like "Sold out", "Selling fast", "Few tickets left", "Limited spots", etc.
          If no capacity/availability info is shown, return an empty string for 'capacity'.

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


def crawl_paged(source_cfg: dict, openai_key: str, today: datetime.date) -> list:
    all_events = []
    page_num = int(source_cfg["start_page"])
    pages_crawled = 0

    base_url = source_cfg["base_url"]
    page_param = source_cfg["page_param"]
    max_pages = int(source_cfg["max_pages"])
    stop_mode = source_cfg["stop_mode"]
    safety_max_pages = int(source_cfg["safety_max_pages"])
    wait_selector = source_cfg["wait_selector"]
    html_output_file = source_cfg["html_output_file"]

    while True:
        if stop_mode == "max_pages" and page_num > max_pages:
            break
        if stop_mode == "until_empty" and pages_crawled >= safety_max_pages:
            print(f"Reached safety_max_pages={safety_max_pages}. Stopping.")
            break

        url = build_url_with_page_param(base_url, page_param, page_num)

        raw_html = fetch_rendered_html_with_camoufox(
            url=url,
            wait_selector=wait_selector,
            timeout_ms=10000,
            scroll_times=0,
        )

        if SAVE_HTML:
            save_html(raw_html, html_output_file)

        try:
            result = run_smartscraper_on_html(raw_html=raw_html, openai_key=openai_key, today=today)
        except Exception as e:
            print(f"Extraction failed on page {page_num}: {e}")
            break

        events = ensure_list(result)
        print(f"Page {page_num}: extracted {len(events)} events")
        all_events.extend(events)

        pages_crawled += 1

        if stop_mode == "until_empty" and len(events) == 0:
            print("No events found on this page. Stopping.")
            break

        page_num += 1

    return all_events


def crawl_infinite_scroll(source_cfg: dict, openai_key: str, today: datetime.date) -> list:
    base_url = source_cfg["base_url"]
    wait_selector = source_cfg["wait_selector"]
    item_selector = source_cfg.get("item_selector", "") or ""
    html_output_file = source_cfg["html_output_file"]

    raw_html = fetch_rendered_html_with_camoufox(
        url=base_url,
        wait_selector=wait_selector,
        timeout_ms=10000,
        scroll_times=MAX_SCROLLS if not item_selector else 0,
        scroll_pause_sec=SCROLL_PAUSE_SEC,
        scroll_until_no_growth=bool(item_selector),
        item_selector=item_selector,
        no_growth_limit=NO_GROWTH_LIMIT,
    )

    if SAVE_HTML:
        save_html(raw_html, html_output_file)

    result = run_smartscraper_on_html(raw_html=raw_html, openai_key=openai_key, today=today)
    events = ensure_list(result)
    print(f"Infinite scroll: extracted {len(events)} events")
    return events


def main():
    load_dotenv()
    openai_key = os.getenv("OPENAI_API_KEY")

    if not openai_key:
        print("Error: Could not find API key. Did you create the .env file?")
        raise SystemExit(1)

    today = datetime.date.today()

    all_events = []

    enabled_sources = [name for name, cfg in SOURCES.items() if cfg.get("enabled")]
    if not enabled_sources:
        print("No sources enabled. Set SOURCES[...]['enabled'] = True.")
        raise SystemExit(1)

    for source_name in enabled_sources:
        cfg = SOURCES[source_name]
        print(f"\n=== Scraping source: {source_name} ({cfg['crawl_strategy']}) ===")

        if cfg["crawl_strategy"] == "paged":
            events = crawl_paged(source_cfg=cfg, openai_key=openai_key, today=today)
        elif cfg["crawl_strategy"] == "infinite_scroll":
            events = crawl_infinite_scroll(source_cfg=cfg, openai_key=openai_key, today=today)
        else:
            raise ValueError(f"Unknown crawl_strategy for {source_name}: {cfg['crawl_strategy']}")

        # Tag source for downstream filtering/debugging
        for ev in events:
            if isinstance(ev, dict) and "source" not in ev:
                ev["source"] = source_name

        all_events.extend(events)

    all_events = dedupe_events(all_events)
    print(f"\nTotal events after dedupe: {len(all_events)}")

    with open("events.json", "w", encoding="utf-8") as json_file:
        json.dump(all_events, json_file, indent=4, ensure_ascii=False)

    print("Success! Data saved to events.json")
    print(f"Total number of events scraped: {len(all_events)}")


if __name__ == "__main__":
    main()
