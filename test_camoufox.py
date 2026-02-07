from camoufox import Camoufox


def get_html_from_camoufox(client, url: str) -> str:
    # Try common method names to retrieve HTML from Camoufox-like clients
    if hasattr(client, "get_html") and callable(getattr(client, "get_html")):
        return client.get_html(url)
    for name in ("get", "fetch_html", "fetch", "html", "get_page_source"):
        if hasattr(client, name) and callable(getattr(client, name)):
            return getattr(client, name)(url)
    if hasattr(client, "navigate") and callable(getattr(client, "navigate")):
        client.navigate(url)
        if hasattr(client, "page_source"):
            src = getattr(client, "page_source")
            return src() if callable(src) else src
    raise AttributeError("Camoufox does not provide a known HTML retrieval method (tried: get_html, get, fetch_html, fetch, html, get_page_source, navigate+page_source).")


def main():
    # Initialize Camoufox
    camoufox = Camoufox()

    # Define the target URL
    target_url = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"

    # Fetch the raw HTML via a compatible method
    html_content = get_html_from_camoufox(camoufox, target_url)

    # Save the raw HTML to a text file
    with open("eventbrite_page.txt", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("Saved HTML content to eventbrite_page.txt")


if __name__ == "__main__":
    main()
