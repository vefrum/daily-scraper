import nest_asyncio
import os
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
import json

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
    "headless": False,
}

# 3. Define the Target
target_url = "https://www.eventbrite.sg/d/singapore--singapore/business--events/"
prompt = "Compile a list of events on the page. Return its 'title', 'date', 'location', 'url' as JSON."

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
    if "document" in state:
        print(str(state["document"])[:500]) 
    else:
        print("Could not find document in state.")

    # Save full version to file
    document_content = state.get("document", "No document found")
    if document_content:
        with open("raw_scrape_data.txt", "w", encoding="utf-8") as f:
            f.write(str(document_content))
    else:
        print("Document content is empty.")

    print("\nSaved full robot memory to raw_scrape_data.txt")

    # 6. Save the Result
    with open("events.json", "w", encoding="utf-8") as json_file:
        if isinstance(result, list):
            json.dump(result, json_file, indent=4)
        else:
            print("Result is not a list. Please check the scraper output.")
        
    print("Success! Data saved to events.json")

except Exception as e:
    print(f"Error occurred: {e}")
