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

# Initialize SmartScraperGraph with the configuration and target URL
scraper = SmartScraperGraph(config, "https://www.eventbrite.sg/d/singapore--singapore/business--events/")

# Perform the scraping
result = scraper.scrape()

# Save the final result to events.json
with open("events.json", "w") as json_file:
    json_file.write(result)
