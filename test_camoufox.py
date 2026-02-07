from camoufox import Camoufox


def main():
    # Initialize Camoufox
    camoufox = Camoufox()

    # Define the target URL
    target_url = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"

    # Fetch the raw HTML
    html_content = camoufox.get_html(target_url)

    # Save the raw HTML to a text file
    with open("eventbrite_page.txt", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("Saved HTML content to eventbrite_page.txt")


if __name__ == "__main__":
    main()
