# Multi-source Event Scraper (Camoufox + SmartScraperGraph)

This project scrapes event listings into a single `events.json` file using:
- **Camoufox** (Playwright-based browser) to render pages (including JS-heavy / SPA sites)
- **SmartScraperGraph** to extract structured event data from the rendered HTML

## Quick start

### 1) Setup environment
Create a virtual environment (recommended):

- macOS/Linux:
  - `python -m venv .venv`
  - `source .venv/bin/activate`

- Windows (PowerShell):
  - `python -m venv .venv`
  - `.venv\Scripts\Activate.ps1`

### 2) Install dependencies
- `pip install -r requirements.txt`

### 3) Add API key
Create a `.env` file in the repo root:
- `OPENAI_API_KEY=your_key_here`

### 4) Run
- `python camoufox_smartscraper.py`

Output:
- `events.json`

## Configuration

Edit the config section at the top of `camoufox_smartscraper.py`.

### Crawl strategy

#### A) Paged crawl
Use this when the site supports pagination via query param (e.g. `?page=2`, `?p=2`).

- `CRAWL_STRATEGY = "paged"`
- `PAGE_PARAM = "page"` (change to `"p"` for Peatix, etc.)
- `MAX_PAGES = 2` (debug cap)
- `STOP_MODE = "max_pages"` or `"until_empty"`

#### B) Infinite scroll crawl
Use this when the site loads more events as you scroll.

- `CRAWL_STRATEGY = "infinite_scroll"`

There are 2 ways to scroll:

1) **Fixed scroll count** (simple, but can be slow)
- Leave `CARD_SELECTOR = ""`
- Set `MAX_SCROLLS` and `SCROLL_PAUSE_SEC`

2) **Adaptive scroll (recommended)** (stops early when no new cards load)
- Set `CARD_SELECTOR` to a CSS selector that matches each event card
- The script will stop when the number of matched cards stops increasing for `NO_GROWTH_LIMIT` rounds
- `MAX_SCROLLS` still acts as a safety cap

### Saving rendered HTML (for inspection)

To save the rendered HTML to disk:
- Set `SAVE_HTML = True`
- It will write to `HTML_OUTPUT_FILE` (default: `scraped_page.html`)

This is useful to understand how the site structures its event cards, even if the HTML is large/noisy.

## How to find a CARD_SELECTOR

1. Set:
   - `SAVE_HTML = True`
   - `CRAWL_STRATEGY = "infinite_scroll"`
   - (optional) keep `MAX_SCROLLS` small first, like 5–10, just to generate a sample

2. Run:
   - `python camoufox_smartscraper.py`

3. Open `scraped_page.html` in your browser and use DevTools (Inspect Element) on an event card.

4. Pick a selector that matches all cards.
   - Prefer stable attributes like `data-testid`, `data-*`, or consistent container classes.
   - Avoid highly random-looking class names if possible.

5. Set `CARD_SELECTOR` and rerun. The crawler should now stop early once no new cards load.

## Notes
- Some sites may show cookie banners or region popups; if extraction looks empty, enable `SAVE_HTML` and inspect what was actually rendered.
- `capacity` is extracted as a best-effort field (e.g. “Sold out”, “Selling fast”, “Almost full”). If not present, it will be an empty string.
