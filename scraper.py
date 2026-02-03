from scrapegraphai.graphs import SmartScraperGraph
import nest_asyncio

# Apply nest_asyncio to handle asyncio properly
nest_asyncio.apply()

# Set up the configuration for Ollama
config = {
    "model": "deepseek-coder-v2",
    "base_url": "http://localhost:11434",
    "format": "json"
}

# Initialize SmartScraperGraph with the configuration
scraper = SmartScraperGraph(config)

# Target URL for scraping events
target_url = "https://www.eventbrite.sg/d/singapore--singapore/business--events/"

# Prompt to extract event details
prompt = """Extract a list of 5 business/career events. For each event, extract the 'title', 'date', 'location', and 'link'. Return the result as a JSON list."""

# Perform the scraping
result = scraper.scrape(target_url, prompt)

# Save the final result to events.json
with open("events.json", "w") as json_file:
    json_file.write(result)
