"""Opens the Streamlit app in a real browser so it registers as an active
visitor (a plain HTTP ping only fetches the static shell, not a WebSocket
session, so it never resets Streamlit Cloud's sleep timer). If the app is
already asleep, clicks the "wake up" button and waits for it to boot.
"""
import sys

from playwright.sync_api import sync_playwright

APP_URL = "https://orientos-cordis-analysis.streamlit.app/"
WAKE_BUTTON_TEXT = "get this app back up"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(APP_URL, wait_until="networkidle", timeout=60000)

        wake_button = page.get_by_text(WAKE_BUTTON_TEXT, exact=False)
        if wake_button.count() > 0:
            print("App is asleep, clicking wake-up button...")
            wake_button.first.click()
            page.wait_for_timeout(20000)
            page.wait_for_load_state("networkidle", timeout=60000)
            print("Wake-up triggered.")
        else:
            print("App already awake.")

        browser.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Wake check failed: {exc}", file=sys.stderr)
        sys.exit(1)
