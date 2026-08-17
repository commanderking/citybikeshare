from playwright.sync_api import Browser, Error as PlaywrightError, Playwright

# Playwright's PyPI package ships only the Python client; the browser binaries are a
# separate per-machine download. Playwright's own error suggests a bare
# `playwright install`, which resolves outside the Poetry-managed venv.
INSTALL_COMMAND = "poetry run playwright install chromium"


def _raise_if_chromium_missing(error: PlaywrightError) -> None:
    # Matching Playwright's message beats pre-checking `chromium.executable_path`:
    # headless mode launches chromium_headless_shell, a different binary from the one
    # that property resolves to, so a path check can pass while the launch still fails.
    if "playwright install" not in str(error):
        return

    raise RuntimeError(
        f"Playwright's Chromium browser is not installed. Install it with:\n"
        f"    {INSTALL_COMMAND}"
    ) from error


def launch_chromium(playwright: Playwright) -> Browser:
    """Launch headless Chromium, reporting a missing browser as an actionable error."""
    try:
        return playwright.chromium.launch(headless=True)
    except PlaywrightError as error:
        _raise_if_chromium_missing(error)
        raise
