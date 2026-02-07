import nest_asyncio
import os
import json
import datetime
from dotenv import load_dotenv

from camoufox.sync_api import Camoufox
from scrapegraphai.graphs import SmartScraperGraph

# Prevent event loop errors in some environments (e.g. notebooks)
nest_asyncio.apply()

def fetch_rendered_html_with_camoufox(url: str, wait_selector: str = "div.event-list", timeout_ms: int = 10000) -> str:
    """
    Uses Camoufox to load a JS-heavy page and returns the fully rendered HTML.
    """
    with Camoufox(headless=True) as browser:
        page = browser.new_page()

        print("Navigating...")
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
    state = scraper.get_state()

    return result, state


def main():
    load_dotenv()
    openai_key = os.getenv("OPENAI_API_KEY")

    if not openai_key:
        print("Error: Could not find API key. Did you create the .env file?")
        raise SystemExit(1)

    today = datetime.date.today()
    target_url = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"

    # 1) Render with Camoufox and capture HTML
    raw_html = fetch_rendered_html_with_camoufox(
        url=target_url,
        wait_selector="div.event-list",
        timeout_ms=10000
    )

    with open("scraped_page.html", "w", encoding="utf-8") as f:
        f.write(raw_html)
    print(f"Captured {len(raw_html)} characters of HTML. Saved to scraped_page.html")

    # 2) Extract with SmartScraperGraph from the rendered HTML
    try:
        result, state = run_smartscraper_on_html(raw_html=raw_html, openai_key=openai_key, today=today)

        # Debug: what the robot saw
        print("\n--- WHAT THE ROBOT SAW (First 500 chars) ---")
        if "doc" in state:
            print(str(state["doc"])[:500])
        else:
            print("Could not find document in state.")

        # Save full doc
        document_content = state.get("doc", "")
        if document_content:
            with open("raw_scrape_data.txt", "w", encoding="utf-8") as f:
                f.write(str(document_content))
            print("\nSaved full robot memory to raw_scrape_data.txt")
        else:
            print("Document content is empty or not found.")

        # Save answer
        ans_content = state.get("answer", "")
        if ans_content:
            with open("answer_data.txt", "w", encoding="utf-8") as f:
                f.write(str(ans_content))
            print("Saved answer to answer_data.txt")
        else:
            print("Ans content is empty or not found.")

        # Save final JSON
        with open("events.json", "w", encoding="utf-8") as json_file:
            if isinstance(result, list):
                json.dump(result, json_file, indent=4, ensure_ascii=False)
            else:
                print("Result is not a list. Attempting to wrap in a list.")
                json.dump([result], json_file, indent=4, ensure_ascii=False)

        print("Success! Data saved to events.json")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
