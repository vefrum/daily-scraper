from camoufox.sync_api import Camoufox
import time

# 1. Initialize the Stealth Browser
with Camoufox(headless=True) as browser:
    page = browser.new_page()
    
    # 2. Go to the target URL
    print("Navigating...")
    page.goto("https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1") # Example URL

    # 3. CRITICAL: Wait for the dynamic content to appear
    # Don't just sleep; wait for the specific container that holds the tickets/events.
    # Inspect the site and find a class like '.event-list', '.ticket-types', etc.
    try:
        # Wait up to 10 seconds for the element to appear
        page.wait_for_selector("div.event-list", timeout=10000) 
        print("Content loaded!")
    except:
        print("Timed out waiting for selector, grabbing whatever is there...")

    # 4. Fetch the fully rendered HTML
    raw_html = page.content()

    # 5. (Optional) Save it to a file to verify what you got
    with open("scraped_page.html", "w", encoding="utf-8") as f:
        f.write(raw_html)

    print(f"Captured {len(raw_html)} characters of HTML.")

# Now 'raw_html' is a string you can pass directly to SmartScraperGraph