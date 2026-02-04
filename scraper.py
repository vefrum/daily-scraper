import nest_asyncio
from scrapegraphai.graphs import SmartScraperGraph
import json

# 1. Apply the patch to prevent loop errors
nest_asyncio.apply()

# 2. Define the Configuration (The AI deleted this part!)
config = {
    "llm": {
        "model": "ollama/deepseek-coder-v2",
        "base_url": "http://localhost:11434",
        "format": "json"
    },
    "verbose": True,
}

# 3. Define the Target
target_url = "https://www.eventbrite.sg/d/singapore--singapore/business--events/"
prompt = "Find the first business event on the page. Return its 'title' and 'date' as JSON."

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
    
    # 6. Save the Result
    with open("events.json", "w", encoding="utf-8") as json_file:
        json.dump(result, json_file, indent=4)
        
    print("Success! Data saved to events.json")

except Exception as e:
    print(f"Error occurred: {e}")