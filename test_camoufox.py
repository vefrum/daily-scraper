from camoufox import Camoufox


def main():
    # Initialize Camoufox
    camoufox = Camoufox()

    # Define the target URL
    target_url = "https://www.eventbrite.sg/d/singapore--singapore/all-events/?page=1"

    # Fetch the raw HTML
    html_content = camoufox.get_html(target_url)

    # Print the raw HTML
    print(html_content)


if __name__ == "__main__":
    main()
