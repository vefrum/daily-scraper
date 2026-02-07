import nest_asyncio
import os
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
import json
import datetime

# 1. Apply the patch to prevent loop errors
nest_asyncio.apply()

# Load the secrets from the .env file
load_dotenv()

# Get the key
openai_key = os.getenv("OPENAI_API_KEY")

# Check if it worked (Optional safety check)
if not openai_key:
    print("Error: Could not find API key. Did you create the .env file?")
    exit()

# 2. Define the Configuration (The AI deleted this part!)
config = {
    "llm": {
        "api_key": openai_key,
        "model": "openai/gpt-5-mini"
    },
    "verbose": True,
    "headless": True,
    "loader_kwargs": {
        # 1. Force use of your LOCAL Google Chrome (Bypasses "Bot" fingerprint)
        "channel": "chrome", 
        
        # 2. Use the absolute latest User-Agent (Feb 2026 Compatible)
        "args": [
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "--disable-blink-features=AutomationControlled", # Hides the "Robot" flag
            "--no-sandbox",
            "--start-maximized" # Sometimes small windows trigger "Mobile" views
        ]
    }
}

# 3. Define the Target
today = datetime.date.today()
target_url = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"
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

# 4. Initialize the Scraper
# This connects the 'config' (brain) with the 'url' (target)
scraper = SmartScraperGraph(
    prompt=prompt,
    source=target_url,
    config=config
)

# 5. Run the Robot
try:
    print("Starting scrape... (This might take a minute)")
    result = scraper.run()
    
    # Get the memory of the robot
    state = scraper.get_state()

    # Print short version to console
    print("\n--- WHAT THE ROBOT SAW (First 500 chars) ---")
    if "doc" in state:
        print(str(state["doc"])[:500]) 
    else:
        print("Could not find document in state.")

    # Save full version to file
    document_content = state.get("doc", "")
    if document_content:
        with open("raw_scrape_data.txt", "w", encoding="utf-8") as f:
            f.write(str(document_content))
    else:
        print("Document content is empty or not found.")

    print("\nSaved full robot memory to raw_scrape_data.txt")

    # Save answer to file
    ans_content = state.get("answer", "")
    if ans_content:
        with open("answer_data.txt", "w", encoding="utf-8") as f:
            f.write(str(ans_content))
    else:
        print("Ans content is empty or not found.")

    print("\nSaved full robot memory to raw_scrape_data.txt")

    # 6. Save the Result
    with open("events.json", "w", encoding="utf-8") as json_file:
        if isinstance(result, list):
            json.dump(result, json_file, indent=4, ensure_ascii=False)
        else:
            print("Result is not a list. Attempting to wrap in a list.")
            json.dump([result], json_file, indent=4, ensure_ascii=False)
        
    print("Success! Data saved to events.json")

except Exception as e:
    print(f"Error occurred: {e}")
