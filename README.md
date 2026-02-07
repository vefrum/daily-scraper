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

### Max scrolls vs max pages

- `MAX_SCROLLS` is a **global** setting used for infinite scroll sources.
- `max_pages` is a **per-source** setting inside `SOURCES` for paged sources.
- If a paged source does not specify `max_pages`, the script uses `DEFAULT_MAX_PAGES`.

### Crawl strategy

#### A) Paged crawl
Use this when the site supports pagination via query param (e.g. `?page=2`, `?p=2`).

Per source, set:
- `crawl_strategy = "paged"`
- `page_param = "page"` (change to `"p"` for Peatix, etc.)
- `max_pages = 2` (debug cap)
- `stop_mode = "max_pages"` or `"until_empty"`

#### B) Infinite scroll crawl
Use this when the site loads more events as you scroll.

Per source, set:
- `crawl_strategy = "infinite_scroll"`

There are 2 ways to scroll:

1) **Fixed scroll count** (simple, but can be slow)
- Leave `item_selector = ""`
- The script will scroll `MAX_SCROLLS` times

2) **Adaptive scroll (recommended)** (stops early when no new items load)
- Set `item_selector` to a CSS selector that matches each event item/card
- The script will stop when the number of matched elements stops increasing for `NO_GROWTH_LIMIT` rounds
- `MAX_SCROLLS` still acts as a safety cap

Examples:
- Luma: `.card-wrapper` (ignore the `jsx-123...` class, it’s usually auto-generated)
- Fever: `[data-testid^="fv-plan-card"]` (matches any element whose `data-testid` starts with `fv-plan-card`)

### Saving rendered HTML (for inspection)

To save the rendered HTML to disk:
- Set `SAVE_HTML = True`
- Each source writes to its own `html_output_file` (e.g. `scraped_luma.html`)

This is useful to understand how the site structures its event cards, even if the HTML is large/noisy.

## How to find an item_selector

1. Set:
   - `SAVE_HTML = True`
   - Enable only the source you’re debugging (set others `enabled: False`)

2. Run:
   - `python camoufox_smartscraper.py`

3. Open the saved HTML file (e.g. `scraped_luma.html`) in your browser and use DevTools (Inspect Element) on an event card.

4. Pick a selector that matches all cards.
   - Prefer stable attributes like `data-testid`, `data-*`, or consistent container classes.
   - Avoid highly random-looking class names if possible.

5. Set `item_selector` and rerun. The crawler should now stop early once no new items load.

## Notes
- Some sites may show cookie banners or region popups; if extraction looks empty, enable `SAVE_HTML` and inspect what was actually rendered.
- `capacity` is extracted as a best-effort field (e.g. “Sold out”, “Selling fast”, “Almost full”). If not present, it will be an empty string.
