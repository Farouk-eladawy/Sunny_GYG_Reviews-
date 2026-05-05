"""
GetYourGuide → Airtable Sync (Multi-Account)

This script supports running multiple GetYourGuide supplier accounts in parallel with full session isolation:
- Separate persistent browser profiles per account under .profiles/
- Separate user-data-dir per browser and a unique remote debugging port per account
- Per-account cache/state/log/diagnostics/report files (no shared writes)
- Tab hygiene per cycle: closes all extra tabs and keeps only one tab open
"""

import os
import json
import time
import random
import threading
import multiprocessing as mp
import re
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import requests
import pyotp

# Load environment variables (can be disabled for tests/sandboxes)
_disable_dotenv = str(os.getenv("DISABLE_DOTENV", "")).strip().lower() in ("1", "true", "yes", "on")
if not _disable_dotenv:
    try:
        load_dotenv(dotenv_path=str(Path(__file__).with_name(".env")), override=False)
    except:
        load_dotenv(override=False)

# Configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
GYG_EMAIL = os.getenv("GYG_EMAIL")
GYG_PASSWORD = os.getenv("GYG_PASSWORD")
GYG_2FA_SECRET = os.getenv("GYG_2FA_SECRET")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
BROWSER_MODE = os.getenv("BROWSER_MODE", os.getenv("HEADLESS_MODE", "launch")).strip().lower()
BROWSER_TYPE = os.getenv("BROWSER_TYPE", "chromium").strip().lower()
CDP_URL = os.getenv("CDP_URL", "http://127.0.0.1:9222").strip()

ACCOUNT_COUNT = int(os.getenv("ACCOUNT_COUNT", "1").strip() or "1")
PERSISTENT_PROFILE = str(os.getenv("PERSISTENT_PROFILE", "false")).strip().lower() in ("1", "true", "yes", "on")
PROFILES_DIR = Path(os.getenv("PROFILES_DIR", ".profiles")).resolve()
PROFILE_NAME_TEMPLATE = os.getenv("PROFILE_NAME_TEMPLATE", "account_{index}").strip() or "account_{index}"
BROWSER_PORT_BASE = int(os.getenv("BROWSER_PORT_BASE", "9300").strip() or "9300")
KEEP_BROWSERS_OPEN = str(
    os.getenv("KEEP_BROWSERS_OPEN", "true" if ACCOUNT_COUNT > 1 else "false")
).strip().lower() in ("1", "true", "yes", "on")

CACHE_FILE = "sent_reviews_cache.json"
REPLIED_CACHE_FILE = "replied_reviews_cache.json"
STATE_FILE = "auth_state.json"
LOG_FILE = "sync_log.txt"
DIAGNOSTICS_DIR = Path("diagnostics")

_AIRTABLE_WRITE_LOCK = threading.Lock()
_CACHE_WRITE_LOCK = threading.Lock()
_REPLIED_CACHE_WRITE_LOCK = threading.Lock()
_REPORT_WRITE_LOCK = threading.Lock()
MAX_REPEATS = 10
MAX_PAGES = 5 # Safety limit, but script will auto-stop
CYCLE_DELAY_MINUTES = int(os.getenv("CYCLE_DELAY_MINUTES", "1")) # Wait time between full cycles
FAILURE_CYCLE_DELAY_MINUTES = 5
SESSION_RECOVERY_RETRIES = 3
SECURITY_AUTO_WAIT_SECONDS = int(os.getenv("SECURITY_AUTO_WAIT_SECONDS", "60"))
SECURITY_INTERACTIVE_WAIT_SECONDS = int(os.getenv("SECURITY_INTERACTIVE_WAIT_SECONDS", "240"))
SECURITY_BACKOFF_MINUTES = int(os.getenv("SECURITY_BACKOFF_MINUTES", "30"))
SECURITY_REFRESH_ATTEMPTS = int(os.getenv("SECURITY_REFRESH_ATTEMPTS", "2"))
SECURITY_REFRESH_INTERVAL_SECONDS = int(os.getenv("SECURITY_REFRESH_INTERVAL_SECONDS", "25"))
SECURITY_STABILITY_SECONDS = int(os.getenv("SECURITY_STABILITY_SECONDS", "6"))
SECURITY_BOUNCE_LIMIT = int(os.getenv("SECURITY_BOUNCE_LIMIT", "3"))

class CycleRestartRequested(Exception):
    """Raised to abort the current cycle and restart after a backoff delay."""
    def __init__(self, delay_minutes, reason):
        super().__init__(reason)
        self.delay_minutes = delay_minutes
        self.reason = reason

def is_truthy(value):
    """Parses common truthy strings from environment variables."""
    return str(value).strip().lower() in ("1", "true", "yes", "on", "headless", "hidden", "background")

def get_account_env(var_name, account_index):
    """
    Returns the account-scoped environment variable (e.g. VAR_1) if present,
    otherwise falls back to the unscoped VAR.
    """
    if account_index is None:
        return os.getenv(var_name)
    return os.getenv(f"{var_name}_{account_index}") or os.getenv(var_name)

def get_profile_root(account_index):
    """
    Returns the root directory for the given account profile under PROFILES_DIR.
    """
    profile_name = PROFILE_NAME_TEMPLATE.format(index=account_index)
    return (PROFILES_DIR / profile_name).resolve()

def configure_account_globals(account_index):
    """
    Configures per-account global variables and per-account file paths.

    عزل الجلسات بين الحسابات يتم عبر:
    - user-data-dir مستقل لكل حساب داخل .profiles
    - ملفات حالة/كاش/لوج وتقارير مستقلة لكل حساب
    - منفذ remote-debugging مختلف لكل متصفح
    """
    global AIRTABLE_TABLE_NAME, GYG_EMAIL, GYG_PASSWORD, GYG_2FA_SECRET
    global CACHE_FILE, REPLIED_CACHE_FILE, STATE_FILE, LOG_FILE, DIAGNOSTICS_DIR

    AIRTABLE_TABLE_NAME = get_account_env("AIRTABLE_TABLE_NAME", account_index)
    GYG_EMAIL = get_account_env("GYG_EMAIL", account_index)
    GYG_PASSWORD = get_account_env("GYG_PASSWORD", account_index)
    GYG_2FA_SECRET = get_account_env("GYG_2FA_SECRET", account_index)

    profile_root = get_profile_root(account_index)
    data_dir = profile_root / "data"
    logs_dir = profile_root / "logs"
    diagnostics_dir = profile_root / "diagnostics"
    reports_dir = profile_root / "reports"

    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    CACHE_FILE = str((data_dir / "sent_reviews_cache.json").resolve())
    REPLIED_CACHE_FILE = str((data_dir / "replied_reviews_cache.json").resolve())
    STATE_FILE = str((data_dir / "auth_state.json").resolve())
    LOG_FILE = str((logs_dir / "sync_log.txt").resolve())
    DIAGNOSTICS_DIR = diagnostics_dir

def build_account_plan(account_index):
    """
    Builds a non-mutating account plan (paths/ports/table name) for validation/tests.
    """
    profile_root = get_profile_root(account_index)
    return {
        "account_index": account_index,
        "profile_root": str(profile_root),
        "user_data_dir": str(get_account_browser_user_data_dir(account_index)),
        "remote_debugging_port": int(get_account_browser_port(account_index)),
        "airtable_table_name": get_account_env("AIRTABLE_TABLE_NAME", account_index),
    }

def get_account_browser_user_data_dir(account_index):
    """
    Returns a per-account Chrome/Chromium user-data-dir path.
    """
    return (get_profile_root(account_index) / "browser_user_data").resolve()

def get_account_browser_port(account_index):
    """
    Returns a unique remote debugging port for each account browser.
    """
    return int(BROWSER_PORT_BASE) + int(account_index)

def get_account_cdp_url(account_index):
    """
    Returns the CDP URL for a given account.

    - If CDP_URL_{i} exists, it is used.
    - Else if ACCOUNT_COUNT==1 and CDP_URL exists, it is used.
    - Else it is derived from the per-account port (http://127.0.0.1:{BROWSER_PORT_BASE+i}).
    """
    scoped = os.getenv(f"CDP_URL_{account_index}")
    if scoped:
        return scoped.strip()
    if ACCOUNT_COUNT <= 1 and CDP_URL:
        return (CDP_URL or "").strip()
    port = get_account_browser_port(account_index)
    return f"http://127.0.0.1:{port}"

def atomic_write_json(path, data):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def get_effective_browser_mode():
    headless_env = os.getenv("HEADLESS_MODE", "false")
    headless_requested = is_truthy(headless_env)
    mode = BROWSER_MODE

    if mode in ("true", "false"):
        mode = "headless" if headless_requested else "launch"

    if mode == "cdp" and headless_requested:
        return "headless"

    if mode in ("background", "hidden"):
        return "headless"

    return mode or ("headless" if headless_requested else "launch")

def get_browser_channel():
    if BROWSER_TYPE == "edge":
        return "msedge"
    if BROWSER_TYPE == "chrome":
        return "chrome"
    return None

def build_reviews_url(page_index):
    if page_index <= 0:
        return "https://supplier.getyourguide.com/performance/reviews"
    return f"https://supplier.getyourguide.com/performance/reviews?page={page_index}"

def ensure_reviews_loaded(page, page_index, retries=3):
    target_url = build_reviews_url(page_index)
    for attempt in range(1, retries + 1):
        try:
            if detect_security_interstitial(page):
                handle_security_interstitial(page, get_effective_browser_mode() == "headless", f"reviews_page_{page_index + 1}")
            page.wait_for_selector('[data-testid="review-card"]', timeout=10000)
            return True
        except CycleRestartRequested:
            raise
        except:
            current_url = page.url
            has_error = (
                page.locator("text=An error occurred").count() > 0 or
                page.locator("text=We’ll be right back").count() > 0 or
                page.locator("text=Something wrong happened").count() > 0
            )
            if has_error:
                log(f"Reviews page error detected on attempt {attempt}/{retries}. Reloading {target_url}", "WARNING")
            else:
                log(f"Reviews did not load on attempt {attempt}/{retries}. Current URL: {current_url}", "WARNING")
            try:
                navigate_with_delay(page, target_url, 2, 4)
            except Exception as e:
                log(f"Retry navigation failed: {e}", "WARNING")
    return False

def create_browser_session(playwright_instance, is_headless):
    effective_mode = get_effective_browser_mode()
    channel = get_browser_channel()

    def build_context_args(use_storage_state=True):
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "viewport": {"width": 1920, "height": 1080}
        }

        if use_storage_state and STATE_FILE and os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    json.load(f)
                log("Loading saved session state...", "INFO")
                context_args["storage_state"] = STATE_FILE
            except Exception as e:
                log(f"Saved session state is corrupt or invalid: {e}. Starting fresh.", "WARNING")
        return context_args

    def recreate_fresh_context(browser, context, owns_browser):
        if owns_browser:
            try:
                context.close()
            except:
                pass
            new_context = browser.new_context(**build_context_args(use_storage_state=False))
            new_page = new_context.new_page()
            return new_context, new_page
        return context, context.new_page()

    if effective_mode == "cdp":
        log(f"Connecting to existing browser via CDP: {CDP_URL}")
        browser = playwright_instance.chromium.connect_over_cdp(CDP_URL, timeout=60000)
        context = browser.contexts[0] if browser.contexts else browser.new_context(**build_context_args(use_storage_state=False))
        page = context.new_page()
        return browser, context, page, False, recreate_fresh_context

    launch_options = {"headless": is_headless}
    if channel:
        launch_options["channel"] = channel
    try:
        browser = playwright_instance.chromium.launch(**launch_options)
    except Exception as e:
        msg = str(e)
        if channel and ("is not found" in msg.lower() or "distribution" in msg.lower()):
            log(f"Browser channel '{channel}' is not available. Falling back to bundled Chromium.", "WARNING")
            launch_options.pop("channel", None)
            browser = playwright_instance.chromium.launch(**launch_options)
        else:
            raise

    context = browser.new_context(**build_context_args(use_storage_state=True))
    page = context.new_page()
    return browser, context, page, True, recreate_fresh_context

def create_account_browser_session(playwright_instance, is_headless, account_index):
    """
    Creates an isolated browser session for the given account.

    - Persistent profile: uses launch_persistent_context(user_data_dir=...).
    - Non-persistent: uses the legacy launch/new_context flow with storage_state.
    - Always assigns a unique remote debugging port per account to avoid collisions.
    """
    effective_mode = get_effective_browser_mode()
    if effective_mode == "cdp":
        cdp_url = get_account_cdp_url(account_index)
        log(f"Account {account_index}: connecting via CDP: {cdp_url}", "INFO")
        max_attempts = int(os.getenv("CDP_CONNECT_RETRIES", "30") or "30")
        delay_sec = float(os.getenv("CDP_CONNECT_DELAY_SECONDS", "1") or "1")
        browser = None
        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                browser = playwright_instance.chromium.connect_over_cdp(cdp_url, timeout=60000)
                break
            except Exception as e:
                last_error = e
                msg = str(e)
                if "ECONNREFUSED" in msg or "retrieving websocket url" in msg or "ws preparing" in msg:
                    log(f"Account {account_index}: CDP not ready yet (attempt {attempt}/{max_attempts}). Waiting...", "WARNING")
                    time.sleep(max(0.1, delay_sec))
                    continue
                raise
        if not browser:
            raise RuntimeError(f"Account {account_index}: failed to connect to CDP at {cdp_url}. Last error: {last_error}")
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.pages[0] if context.pages else context.new_page()

        def recreate_fresh_context(_browser, _context, _owns_browser):
            try:
                return _context, _context.new_page()
            except:
                return _context, page

        return browser, context, page, False, recreate_fresh_context

    channel = get_browser_channel()
    port = get_account_browser_port(account_index)
    user_data_dir = get_account_browser_user_data_dir(account_index)
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if PERSISTENT_PROFILE:
        launch_options = {"headless": is_headless, "args": [f"--remote-debugging-port={port}"]}
        if channel:
            launch_options["channel"] = channel
        context = playwright_instance.chromium.launch_persistent_context(str(user_data_dir), **launch_options)
        page = context.pages[0] if context.pages else context.new_page()

        def recreate_fresh_context(_browser, _context, _owns_browser):
            try:
                _context.close()
            except:
                pass
            new_context = playwright_instance.chromium.launch_persistent_context(str(user_data_dir), **launch_options)
            new_page = new_context.pages[0] if new_context.pages else new_context.new_page()
            return new_context, new_page

        return context.browser, context, page, True, recreate_fresh_context

    browser, context, page, owns_browser, recreate = create_browser_session(playwright_instance, is_headless)
    return browser, context, page, owns_browser, recreate

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    with _CACHE_WRITE_LOCK:
        atomic_write_json(CACHE_FILE, cache)

def load_replied_cache():
    if os.path.exists(REPLIED_CACHE_FILE):
        try:
            with open(REPLIED_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def save_replied_cache(cache):
    with _REPLIED_CACHE_WRITE_LOCK:
        atomic_write_json(REPLIED_CACHE_FILE, cache)

def log(message, level="INFO"):
    msg = f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {message}"
    print(msg)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except:
        pass

def human_delay(min_sec=2, max_sec=5):
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)

def wait_for_page_settle(page, min_sec=1.5, max_sec=3.5):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except:
        pass
    human_delay(min_sec, max_sec)

def apply_reviews_page_fixes(page):
    try:
        page.add_style_tag(content='''
            iframe[title="Messaging window"],
            iframe[name="Messaging window"],
            iframe[title*="Messaging" i],
            iframe[name*="Messaging" i] {
              display: none !important;
              visibility: hidden !important;
              pointer-events: none !important;
            }
            header.fixed {
              pointer-events: none !important;
            }
        ''')
    except:
        pass

    try:
        page.evaluate("document.body.style.zoom = '65%'")
    except:
        pass

    try:
        page.keyboard.press("Escape")
    except:
        pass

def type_like_human(locator, text, clear_first=True):
    target = locator.first
    target.wait_for(state="visible", timeout=20000)
    try:
        target.scroll_into_view_if_needed()
    except:
        pass
    target.click(delay=random.randint(40, 120))
    human_delay(0.4, 1.1)
    if clear_first:
        target.fill("")
        human_delay(0.2, 0.6)
    for char in text:
        target.type(char, delay=random.randint(90, 220))
        if random.random() < 0.18:
            time.sleep(random.uniform(0.15, 0.55))
    human_delay(0.5, 1.2)

def click_like_human(locator, force=False):
    target = locator.first
    target.wait_for(state="visible", timeout=20000)
    try:
        target.scroll_into_view_if_needed()
    except:
        pass
    try:
        target.hover()
    except:
        pass
    human_delay(0.3, 0.9)
    try:
        target.click(delay=random.randint(50, 160), force=force)
    except Exception as e:
        if "intercepts pointer events" in str(e):
            log("Click intercepted, forcing click via JS...", "WARNING")
            target.evaluate("el => el.click()")
        else:
            raise e
    human_delay(0.7, 1.6)

def navigate_with_delay(page, url, min_sec=2.5, max_sec=5.5):
    page.goto(url, timeout=60000)
    wait_for_page_settle(page, min_sec, max_sec)

def get_body_text_length(page):
    try:
        return page.evaluate("() => (document.body && document.body.innerText ? document.body.innerText.length : 0)")
    except:
        return 0

def is_blank_page(page):
    return get_body_text_length(page) < 50

def has_login_form(page):
    try:
        email_visible = page.locator('input[type="email"]').count() > 0 and page.locator('input[type="email"]').first.is_visible()
    except:
        email_visible = False
    try:
        password_visible = page.locator('input[type="password"]').count() > 0 and page.locator('input[type="password"]').first.is_visible()
    except:
        password_visible = False
    return email_visible or password_visible

def has_captcha(page):
    try:
        return page.locator('iframe[src*="recaptcha"], iframe[src*="cloudflare"]').count() > 0
    except:
        return False

def write_diagnostics(page, tag):
    try:
        out_dir = DIAGNOSTICS_DIR
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_tag = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(tag))[:60]
        base = Path(out_dir) / f"{safe_tag}_{ts}"
        try:
            page.screenshot(path=str(base) + ".png", full_page=True)
        except:
            pass
        try:
            html = page.content()
            (base.with_suffix(".html")).write_text(html, encoding="utf-8", errors="ignore")
        except:
            pass
    except:
        pass

def close_extra_tabs(context, keep_pages=1):
    """
    Closes extra tabs for the current account context to prevent memory growth.
    Keeps exactly one tab open when possible.
    """
    try:
        pages = list(context.pages)
    except:
        return None

    if not pages:
        try:
            return context.new_page()
        except:
            return None

    primary = pages[0]
    for p in pages[keep_pages:]:
        try:
            p.close()
        except:
            pass
    return primary

def ensure_single_tab(context, page):
    primary = close_extra_tabs(context, keep_pages=1) or page
    try:
        if page != primary:
            page = primary
    except:
        page = primary
    return page

def detect_security_interstitial(page):
    try:
        url = (page.url or "").lower()
        if "cdn-cgi/challenge-platform" in url or "cdn-cgi" in url and "challenge" in url:
            return True
        if "verify" in url and "cloudflare" in url:
            return True
    except:
        pass

    try:
        title = (page.title() or "").lower()
        if "just a moment" in title or "attention required" in title:
            return True
    except:
        pass

    try:
        if page.locator("#cf-spinner, #challenge-running, #challenge-form").count() > 0:
            return True
    except:
        pass

    try:
        if page.locator('iframe[src*="challenges.cloudflare.com"]').count() > 0:
            return True
    except:
        pass

    try:
        txt = (page.evaluate("() => (document.body && document.body.innerText ? document.body.innerText : '')") or "").lower()
        if "checking your browser" in txt or "performing security verification" in txt or "verify you are human" in txt:
            return True
    except:
        pass

    return False

def handle_security_interstitial(page, is_headless, stage):
    log(f"Security verification page detected. Stage={stage}, URL={page.url}", "WARNING")
    write_diagnostics(page, f"security_{stage}")

    auto_deadline = None if SECURITY_AUTO_WAIT_SECONDS <= 0 else time.time() + max(5, SECURITY_AUTO_WAIT_SECONDS)
    while auto_deadline is None or time.time() < auto_deadline:
        if not detect_security_interstitial(page):
            break
        try:
            page.wait_for_load_state("domcontentloaded", timeout=5000)
        except:
            pass
        time.sleep(2)

    if is_headless:
        for attempt in range(1, max(0, SECURITY_REFRESH_ATTEMPTS) + 1):
            if not detect_security_interstitial(page):
                break
            log(f"Security verification still active. Refresh attempt {attempt}/{SECURITY_REFRESH_ATTEMPTS}...", "WARNING")
            write_diagnostics(page, f"security_{stage}_refresh_{attempt}")
            try:
                page.reload(timeout=60000)
            except:
                try:
                    page.goto(page.url, timeout=60000)
                except:
                    pass
            wait_for_page_settle(page, 2, 4)

            refresh_deadline = time.time() + max(5, SECURITY_REFRESH_INTERVAL_SECONDS)
            while time.time() < refresh_deadline:
                if not detect_security_interstitial(page):
                    break
                time.sleep(2)
            if not detect_security_interstitial(page):
                break

    if is_headless:
        if detect_security_interstitial(page):
            raise CycleRestartRequested(
                SECURITY_BACKOFF_MINUTES,
                f"Blocked by security verification in headless mode. URL={page.url}"
            )

    log("Please complete the security verification in the opened browser window, then wait...", "ACTION")
    interactive_deadline = None if SECURITY_INTERACTIVE_WAIT_SECONDS <= 0 else time.time() + max(10, SECURITY_INTERACTIVE_WAIT_SECONDS)
    while interactive_deadline is None or time.time() < interactive_deadline:
        if not detect_security_interstitial(page):
            break
        try:
            checkbox = page.locator('input[type="checkbox"]')
            if checkbox.count() > 0 and checkbox.first.is_visible():
                if page.locator('input[type="checkbox"]:checked').count() > 0:
                    pass
                else:
                    log("Checkbox detected on verification page. Please tick it if required.", "ACTION")
        except:
            pass
        time.sleep(2)

    if detect_security_interstitial(page):
        raise CycleRestartRequested(
            SECURITY_BACKOFF_MINUTES,
            f"Security verification did not clear in time. URL={page.url}"
        )

    stability_deadline = time.time() + max(0, SECURITY_STABILITY_SECONDS)
    bounce_count = 0
    while time.time() < stability_deadline:
        if detect_security_interstitial(page):
            bounce_count += 1
            if bounce_count > max(0, SECURITY_BOUNCE_LIMIT):
                write_diagnostics(page, f"security_{stage}_bounce")
                raise CycleRestartRequested(
                    SECURITY_BACKOFF_MINUTES,
                    f"Security verification kept returning (bounce). URL={page.url}"
                )
            wait_for_page_settle(page, 2, 4)
            stability_deadline = time.time() + max(0, SECURITY_STABILITY_SECONDS)
        time.sleep(1)
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except:
        pass
    return

def get_login_error_text(page):
    selectors = ['.error-message', '[role="alert"]', 'text=There was an error', 'text=session']
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if loc.count() > 0 and loc.first.is_visible():
                text = (loc.first.inner_text() or "").strip()
                if text:
                    return text
        except:
            pass
    return ""

def clear_bad_session_state(page):
    try:
        page.context.clear_cookies()
    except:
        pass
    try:
        page.evaluate("() => { try { localStorage.clear() } catch(e) {}; try { sessionStorage.clear() } catch(e) {} }")
    except:
        pass
    try:
        if STATE_FILE and os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            log("Deleted corrupt saved session state.", "WARNING")
    except Exception as e:
        log(f"Failed to delete saved session state: {e}", "WARNING")

def wait_for_post_login_state(page, timeout_ms=30000):
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        if detect_security_interstitial(page):
            return "security_check"
        if (
            page.locator("text=An error occurred").count() > 0 or
            page.locator("text=We’ll be right back").count() > 0 or
            page.locator("text=Something wrong happened").count() > 0
        ):
            return "error"
        if page.locator('[data-testid="review-card"]').count() > 0:
            return "reviews"
        if "performance/reviews" in page.url.lower() and not has_login_form(page) and not is_blank_page(page):
            return "reviews"
        if has_login_form(page):
            return "login"
        if has_captcha(page):
            return "captcha"
        if is_blank_page(page):
            time.sleep(1)
            continue
        wait_for_page_settle(page, 0.5, 1.2)
    return "timeout"

def perform_login(page, context, owns_browser, is_headless, max_attempts=3):
    log("Login page detected (Session invalid or expired).")

    try:
        if page.locator("button:has-text('Only essential')").is_visible():
            click_like_human(page.locator("button:has-text('Only essential')"))
            log("Clicked 'Only essential' cookies.")
        elif page.locator("button:has-text('I agree')").is_visible():
            click_like_human(page.locator("button:has-text('I agree')"))
            log("Clicked 'I agree' cookies.")
    except:
        pass

    if GYG_EMAIL and GYG_PASSWORD:
        for attempt in range(1, max(1, max_attempts) + 1):
            log(f"Attempting auto-login (attempt {attempt}/{max_attempts})...")
            try:
                if detect_security_interstitial(page):
                    handle_security_interstitial(page, is_headless, "login_page_entry")

                if is_blank_page(page):
                    log("Login page appears blank. Reloading it first...", "WARNING")
                    navigate_with_delay(page, "https://supplier.getyourguide.com/auth/login", 3, 6)

                if detect_security_interstitial(page):
                    handle_security_interstitial(page, is_headless, "login_page_reload")

                if has_captcha(page):
                    log("CAPTCHA detected on login page.", "WARNING")

                type_like_human(page.locator("input[type='email']"), GYG_EMAIL)
                if page.locator("button:has-text('Continue')").count() > 0:
                    click_like_human(page.locator("button:has-text('Continue')"))

                wait_for_page_settle(page, 1, 2.5)
                type_like_human(page.locator("input[type='password']"), GYG_PASSWORD)
                click_like_human(page.locator("button[type='submit']"))

                wait_for_page_settle(page, 2, 4.5)

                current_url = page.url.lower()
                is_2fa_page = "second-factor" in current_url or "verify-totp" in current_url
                has_otp_inputs = page.locator("input[autocomplete='one-time-code']").count() > 0 or page.locator(".otp-input").count() > 0 or page.locator("input[type='text']").count() >= 6

                if is_2fa_page or has_otp_inputs:
                    log("2FA Challenge detected.")
                    if GYG_2FA_SECRET:
                        totp = pyotp.TOTP(GYG_2FA_SECRET.replace(" ", ""))
                        code = totp.now()
                        log("2FA code generated.", "INFO")

                    inputs = page.locator("input[type='text']")

                    if inputs.count() >= 6:
                        log("Detected split 6-digit input fields.")
                        try:
                            first_input = inputs.nth(0)
                            first_input.wait_for(state="visible", timeout=15000)
                            first_input.click(delay=random.randint(40, 100))
                            human_delay(0.4, 1)

                            for digit in code:
                                page.keyboard.type(digit, delay=random.randint(120, 240))
                                if random.random() < 0.25:
                                    time.sleep(random.uniform(0.2, 0.6))

                            log("Typed 2FA code.")
                        except Exception as e:
                            log(f"Error typing 2FA: {e}", "ERROR")

                    elif page.locator("input[autocomplete='one-time-code']").count() > 0:
                        type_like_human(page.locator("input[autocomplete='one-time-code']"), code)
                        log("Filled single 2FA input.")

                    human_delay(1, 2.5)
                    verify_btn = page.locator("button:has-text('Verify code')")
                    if verify_btn.is_visible():
                        click_like_human(verify_btn)
                        log("Clicked Verify code.")
                    else:
                        log("2FA required but no secret provided in .env", "ERROR")
                        clear_bad_session_state(page)
                        return False

                login_error_text = get_login_error_text(page)
                if login_error_text:
                    log(f"Login error message: {login_error_text}", "ERROR")
                    if "session" in login_error_text.lower() or "log you out" in login_error_text.lower():
                        clear_bad_session_state(page)
                    human_delay(2, 4)
                    continue

                login_state = wait_for_post_login_state(page, 30000)
                if login_state == "security_check":
                    handle_security_interstitial(page, is_headless, "post_login")
                    login_state = wait_for_post_login_state(page, 30000)
                if login_state == "captcha":
                    log("Login blocked by CAPTCHA.", "ERROR")
                    clear_bad_session_state(page)
                    human_delay(3, 6)
                    continue
                if login_state == "error":
                    log(f"GetYourGuide login returned a temporary error page. URL={page.url}", "ERROR")
                    clear_bad_session_state(page)
                    human_delay(3, 6)
                    continue
                if login_state != "reviews":
                    log(f"Login did not complete successfully. State={login_state}, URL={page.url}", "ERROR")
                    clear_bad_session_state(page)
                    human_delay(3, 6)
                    continue

                wait_for_page_settle(page, 2.5, 5)
                log("Login successful.", "SUCCESS")

                if owns_browser:
                    try:
                        context.storage_state(path=STATE_FILE)
                        log("Session state saved.", "INFO")
                    except:
                        pass
                return True
            except Exception as e:
                log(f"Auto-login failed: {e}", "WARNING")
                clear_bad_session_state(page)
                human_delay(3, 6)
                continue
        return False

    log("Please log in manually...", "ACTION")
    try:
        login_state = wait_for_post_login_state(page, 300000)
        if login_state == "security_check":
            handle_security_interstitial(page, is_headless, "manual_login")
            login_state = wait_for_post_login_state(page, 300000)
        if login_state != "reviews":
            raise TimeoutError(f"Unexpected post-login state: {login_state}")
        wait_for_page_settle(page, 2.5, 5)
        if owns_browser:
            context.storage_state(path=STATE_FILE)
        return True
    except:
        log("Login timeout or reviews page not reached.", "ERROR")
        clear_bad_session_state(page)
        return False

def recover_session_for_page(browser, context, page, owns_browser, is_headless, target_page_index, recreate_fresh_context, max_retries=SESSION_RECOVERY_RETRIES):
    target_url = build_reviews_url(target_page_index)
    for attempt in range(1, max_retries + 1):
        log(f"Session lost while targeting page {target_page_index + 1}. Re-login attempt {attempt}/{max_retries}.", "WARNING")

        clear_bad_session_state(page)
        try:
            page.close()
        except:
            pass
        context, page = recreate_fresh_context(browser, context, owns_browser)

        if not perform_login(page, context, owns_browser, is_headless):
            human_delay(2, 4)
            continue

        try:
            navigate_with_delay(page, target_url, 3, 6)
        except Exception as e:
            log(f"Failed to return to page {target_page_index + 1}: {e}", "WARNING")
            human_delay(2, 4)
            continue

        current_url = page.url.lower()
        if "login" in current_url or "signin" in current_url or has_login_form(page):
            log(f"Still redirected to login after retry {attempt}.", "WARNING")
            human_delay(2, 4)
            continue

        if ensure_reviews_loaded(page, target_page_index, retries=2):
            log(f"Recovered session and resumed page {target_page_index + 1}.", "SUCCESS")
            return context, page

    raise CycleRestartRequested(
        FAILURE_CYCLE_DELAY_MINUTES,
        f"Failed to recover session for page {target_page_index + 1} after {max_retries} attempts."
    )

def parse_review_date(date_text):
    if not date_text:
        return None
    
    date_text = date_text.strip()
    
    # Handle "X days/weeks/months ago"
    if "ago" in date_text.lower():
        try:
            parts = date_text.split()
            num = int(parts[0])
            unit = parts[1]
            delta = timedelta(days=0)
            if "day" in unit:
                delta = timedelta(days=num)
            elif "week" in unit:
                delta = timedelta(weeks=num)
            elif "month" in unit:
                delta = timedelta(days=num*30)
            elif "year" in unit:
                delta = timedelta(days=num*365)
            
            return (datetime.now() - delta).strftime("%Y-%m-%d")
        except:
            pass

    # Handle standard dates
    try:
        dt = datetime.strptime(date_text, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except:
        pass
        
    try:
        dt = datetime.strptime(date_text, "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except:
        pass

    return None

_BOOKING_REF_RE = re.compile(r"\bGYG[A-Z0-9]{6,20}\b")

def _safe_inner_text(locator, fallback=""):
    try:
        if locator.count() > 0 and locator.first.is_visible():
            return locator.first.inner_text()
    except:
        pass
    return fallback

def extract_booking_reference_from_text(text):
    if not text:
        return None
    for line in str(text).split("\n"):
        if "Booking reference" in line:
            candidate = line.replace("Booking reference", "").strip().replace(":", "").strip()
            if candidate:
                return candidate
    match = _BOOKING_REF_RE.search(str(text))
    return match.group(0) if match else None

def extract_booking_reference_from_details(page, card):
    opened_pages_before = 0
    try:
        opened_pages_before = len(list(page.context.pages))
    except:
        opened_pages_before = 0

    try:
        expand_btn = card.locator('[data-testid="review-card-expand"]')
        if expand_btn.count() > 0 and expand_btn.first.is_visible():
            click_like_human(expand_btn, force=True)
        else:
            show_details = card.locator('button:has-text("Show details")')
            if show_details.count() > 0 and show_details.first.is_visible():
                click_like_human(show_details, force=True)
    except:
        pass

    try:
        opened_pages_after = len(list(page.context.pages))
        if opened_pages_after > opened_pages_before:
            page = ensure_single_tab(page.context, page)
    except:
        pass

    apply_reviews_page_fixes(page)

    deadline = time.time() + 8
    while time.time() < deadline:
        try:
            dialog = page.locator('[role="dialog"]').last
            dialog_text = _safe_inner_text(dialog, "")
            ref = extract_booking_reference_from_text(dialog_text)
            if ref:
                try:
                    page.keyboard.press("Escape")
                except:
                    pass
                return ref
        except:
            pass

        try:
            body_text = page.evaluate("() => (document.body && document.body.innerText ? document.body.innerText : '')")
            ref = extract_booking_reference_from_text(body_text)
            if ref:
                try:
                    page.keyboard.press("Escape")
                except:
                    pass
                return ref
        except:
            pass

        time.sleep(0.5)

    try:
        pass
    except:
        pass

    try:
        card_text_after = card.inner_text()
        ref = extract_booking_reference_from_text(card_text_after)
        if ref:
            try:
                page.keyboard.press("Escape")
            except:
                pass
            return ref
    except:
        pass

    try:
        body_text = page.evaluate("() => (document.body && document.body.innerText ? document.body.innerText : '')")
        ref = extract_booking_reference_from_text(body_text)
        if ref:
            try:
                page.keyboard.press("Escape")
            except:
                pass
            return ref
    except:
        pass

    try:
        page.keyboard.press("Escape")
    except:
        pass
    return None

def sync_to_airtable(review_data):
    booking_ref = review_data.get("booking_reference")
    if not booking_ref or booking_ref == "N/A":
        log("Invalid Booking Reference", "ERROR")
        return

    # 1. Search for the record
    filter_formula = f"{{Booking Nr.}}='{booking_ref}'"
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        with _AIRTABLE_WRITE_LOCK:
            response = requests.get(url, headers=headers, params={"filterByFormula": filter_formula})
            response.raise_for_status()
            data = response.json()

            if data.get("records"):
                record_id = data["records"][0]["id"]

                update_data = {
                    "fields": {
                        "GYG Rating": int(review_data["general_rating"]),
                        "Customer Review": str(review_data["review_comment"])[:100000],
                        "Review Date": review_data["review_date"]
                    }
                }

                if update_data["fields"]["Review Date"] is None:
                    del update_data["fields"]["Review Date"]

                patch_url = f"{url}/{record_id}"
                patch_response = requests.patch(patch_url, headers=headers, json=update_data)

                if patch_response.status_code == 422:
                    log(f"Airtable 422 Error Payload: {json.dumps(update_data)}", "ERROR")
                    log(f"Airtable Response: {patch_response.text}", "ERROR")

                patch_response.raise_for_status()
                log(f"Updated Airtable for Booking {booking_ref}", "SUCCESS")
            else:
                log(f"Booking {booking_ref} not found in Airtable", "WARNING")

    except Exception as e:
        log(f"Airtable Sync Error: {e}", "ERROR")

def check_airtable_status(booking_ref):
    """
    Checks the Airtable record for 'No show & refund' status.
    Returns 'No Show' if found, otherwise None.
    """
    if not booking_ref or booking_ref == "N/A":
        return None
        
    filter_formula = f"{{Booking Nr.}}='{booking_ref}'"
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, params={"filterByFormula": filter_formula})
        if response.ok:
            data = response.json()
            if data.get("records"):
                fields = data["records"][0].get("fields", {})
                return fields.get("No show & refund")
    except Exception as e:
        log(f"Airtable Check Error: {e}", "ERROR")
    
    return None

def send_negative_review_webhook(review_data):
    if not WEBHOOK_URL:
        log("No Webhook URL provided.", "WARNING")
        return

    try:
        payload = {
            "event": "negative_review_alert",
            "booking_reference": review_data.get("booking_reference"),
            "rating": review_data.get("general_rating"),
            "comment": review_data.get("review_comment"),
            "date": review_data.get("review_date"),
            "timestamp": datetime.now().isoformat()
        }
        
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.ok:
            log("Negative review sent to Webhook.", "SUCCESS")
        else:
            log(f"Webhook failed: {response.status_code}", "ERROR")
    except Exception as e:
        log(f"Webhook Error: {e}", "ERROR")

def generate_reply(review_text, rating, customer_name="Traveler", is_no_show=False):
    if not DEEPSEEK_API_KEY:
        log("No DeepSeek API Key provided.", "WARNING")
        return None

    log(f"Generating AI reply for {rating}-star review (No Show: {is_no_show})...", "AI")
    
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    if is_no_show:
        prompt = f"""
        You are a professional customer service representative for a travel agency.
        The customer '{customer_name}' left a review, but our records show they were a 'No Show'.
        The car arrived at their hotel, but they were not there.
        
        Write a polite, professional reply explaining this situation.
        - Acknowledge their review.
        - Gently clarify that the driver was at the pickup location as scheduled.
        - Maintain a courteous and professional tone.
        - Do not be aggressive.
        
        Review: "{review_text}"
        """
    else:
        prompt = f"""
        You are a professional customer service representative for a travel agency.
        Write a polite, professional, and personalized reply to the following customer review.
        
        Review Details:
        - Customer Name: {customer_name}
        - Rating: {rating} stars
        - Review: "{review_text}"
        
        Guidelines:
        - Keep it concise (under 300 characters if possible).
        - Be grateful for positive feedback.
        - Be empathetic and solution-oriented for negative feedback.
        - Do not use placeholders like [Your Name]. Sign off as "The Team".
        """
    
    data = {
        "model": "deepseek-chat", # or deepseek-coder, usually chat is better for this
        "messages": [
            {"role": "system", "content": "You are a helpful assistant writing responses to customer reviews."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        reply = result['choices'][0]['message']['content'].strip()
        # Remove quotes if AI added them
        if reply.startswith('"') and reply.endswith('"'):
            reply = reply[1:-1]
        log(f"AI Reply Generated: {reply[:50]}...", "SUCCESS")
        return reply
    except Exception as e:
        log(f"DeepSeek API Error: {e}", "ERROR")
        return None

def scrape_cycle_in_session(browser, context, page, owns_browser, recreate_fresh_context, is_headless):
    """
    Executes one scrape cycle using an already-open browser context/page.

    This is the core cycle used by the multi-account worker, so we don't close the browser.
    Returns (context, page, stats_dict).
    """
    cache = load_cache()
    replied_cache = load_replied_cache()
    processed_records = 0
    errors = []

    pending_restart = None
    try:
        log(f"Browser mode: {get_effective_browser_mode()} (visible={not is_headless})")
        log("Opening GetYourGuide Supplier Portal...")
        page = close_extra_tabs(context, keep_pages=1) or page
        try:
            navigate_with_delay(page, build_reviews_url(0), 3, 6)
        except Exception as e:
            errors.append(f"navigation_error: {e}")
            log(f"Error loading page: {e}", "ERROR")
            return context, page, {"processed_records": processed_records, "errors": errors, "status": "error"}

        if detect_security_interstitial(page):
            handle_security_interstitial(page, is_headless, "initial_navigation")

        if is_blank_page(page):
            log("Detected blank page. Reloading before login checks...", "WARNING")
            try:
                navigate_with_delay(page, build_reviews_url(0), 3, 6)
            except Exception as e:
                errors.append(f"blank_recovery_error: {e}")
                log(f"Failed to recover blank page: {e}", "ERROR")
                return context, page, {"processed_records": processed_records, "errors": errors, "status": "error"}

        if detect_security_interstitial(page):
            handle_security_interstitial(page, is_headless, "post_blank_reload")

        current_url = page.url.lower()
        log(f"Current URL: {current_url}")

        is_login_url = "login" in current_url or "signin" in current_url
        has_visible_login_inputs = has_login_form(page)
        are_reviews_visible = page.locator('[data-testid="review-card"]').count() > 0
        is_login_page = (is_login_url or has_visible_login_inputs) and not are_reviews_visible

        if is_login_page:
            if not perform_login(page, context, owns_browser, is_headless, max_attempts=3):
                clear_bad_session_state(page)
                try:
                    page.close()
                except:
                    pass
                context, page = recreate_fresh_context(browser, context, owns_browser)
                navigate_with_delay(page, build_reviews_url(0), 3, 6)
                if not perform_login(page, context, owns_browser, is_headless, max_attempts=3):
                    errors.append("login_failed")
                    return context, page, {"processed_records": processed_records, "errors": errors, "status": "error"}

        else:
            log("Already logged in (Session valid).", "SUCCESS")

        log("Starting scrape...")

        try:
            log("Warm-up navigation: page 2 then back to page 1...", "INFO")
            page = ensure_single_tab(context, page)
            navigate_with_delay(page, build_reviews_url(1), 2, 4)
            apply_reviews_page_fixes(page)
            navigate_with_delay(page, build_reviews_url(0), 2, 4)
            apply_reviews_page_fixes(page)
        except Exception as e:
            log(f"Warm-up navigation skipped due to: {e}", "WARNING")

        for page_num in range(MAX_PAGES):
            log(f"Processing Page {page_num + 1}")

            page = ensure_single_tab(context, page)

            current_url = page.url.lower()
            if "login" in current_url or "signin" in current_url or has_login_form(page):
                context, page = recover_session_for_page(browser, context, page, owns_browser, is_headless, page_num, recreate_fresh_context)

            try:
                if not ensure_reviews_loaded(page, page_num):
                    raise TimeoutError("Reviews failed to load after retries.")

                apply_reviews_page_fixes(page)
                human_delay(0.8, 1.6)
            except CycleRestartRequested:
                raise
            except Exception:
                current_url = page.url.lower()
                if "login" in current_url or "signin" in current_url or has_login_form(page):
                    context, page = recover_session_for_page(browser, context, page, owns_browser, is_headless, page_num, recreate_fresh_context)
                    if not ensure_reviews_loaded(page, page_num):
                        raise CycleRestartRequested(
                            FAILURE_CYCLE_DELAY_MINUTES,
                            f"Could not reload reviews on page {page_num + 1} after session recovery."
                        )
                log("No reviews found or page load error (Timeout).", "WARNING")
                log(f"Debug: Title={page.title()}, URL={page.url}")
                time.sleep(10)
                break

            review_cards = page.locator('[data-testid="review-card"]').all()
            log(f"Found {len(review_cards)} reviews on this page.")

            for index, card in enumerate(review_cards):
                try:
                    try:
                        expand_btn = card.locator('[data-testid="review-card-expand"]')
                        if expand_btn.count() > 0 and expand_btn.first.is_visible():
                            click_like_human(expand_btn, force=True)
                        else:
                            show_details = card.locator('button:has-text("Show details")')
                            if show_details.count() > 0 and show_details.first.is_visible():
                                click_like_human(show_details, force=True)
                    except Exception as e:
                        log(f"Warning: Could not expand review card {index}: {e}", "DEBUG")

                    comment_el = card.locator('[data-testid="review-card-comment"]')
                    comment = comment_el.first.inner_text() if comment_el.count() > 0 else "No comment"

                    booking_ref = None
                    card_text = ""
                    try:
                        booking_ref_el = card.locator('[data-testid="Booking reference"]')
                        if booking_ref_el.count() > 0 and booking_ref_el.first.is_visible():
                            booking_ref = extract_booking_reference_from_text(booking_ref_el.first.inner_text())
                    except:
                        pass

                    if not booking_ref:
                        try:
                            card_text = card.inner_text()
                        except:
                            card_text = "Failed to extract text"
                        booking_ref = extract_booking_reference_from_text(card_text)

                    if not booking_ref:
                        booking_ref = extract_booking_reference_from_details(page, card)
                        if booking_ref:
                            log(f"Booking reference extracted via details: {booking_ref}", "DEBUG")

                    if not booking_ref:
                        log(f"Could not find Booking Reference for review {index}. Skipping.", "WARNING")
                        log(f"Raw card text for review {index}: {card_text[:500]}...", "DEBUG")
                        continue

                    stars_container_loc = card.locator('.c-user-rating__stars-container')
                    rating = 0
                    if stars_container_loc.count() > 0:
                        rating = stars_container_loc.first.locator('.c-user-rating__icon--full').count()

                    date_el = card.locator('.absolute.top-4.right-4')
                    if date_el.count() == 0:
                        date_el = card.locator('text=Reviewed on')

                    review_date_raw = date_el.first.inner_text() if date_el.count() > 0 else ""
                    review_date = parse_review_date(review_date_raw)

                    review_data = {
                        "booking_reference": booking_ref,
                        "review_comment": comment,
                        "general_rating": rating,
                        "review_date": review_date
                    }

                    try:
                        reply_btn = card.locator('button:has-text("Reply")').or_(card.locator('button:has-text("Respond")'))
                        already_replied_ui = card.locator('text=Replied on').count() > 0 or card.locator('text=Response from supplier').count() > 0
                        already_replied_cache = booking_ref in replied_cache

                        if already_replied_ui or already_replied_cache:
                            log(f"Review {booking_ref} already has a reply (UI={already_replied_ui}, Cache={already_replied_cache}). Skipping AI.", "INFO")
                            if already_replied_ui and not already_replied_cache:
                                replied_cache.append(booking_ref)
                                save_replied_cache(replied_cache)

                        elif reply_btn.count() > 0 and reply_btn.first.is_visible():
                            if not reply_btn.first.is_enabled():
                                log(f"Reply button found for {booking_ref} but is DISABLED. Skipping AI.", "INFO")
                                if booking_ref not in replied_cache:
                                    replied_cache.append(booking_ref)
                                    save_replied_cache(replied_cache)
                            else:
                                log(f"Reply button found for {booking_ref}.", "AI_ACTION")

                            should_reply = True
                            is_no_show = False

                            if rating <= 3:
                                log(f"Negative review detected ({rating} stars). Checking Airtable...", "CHECK")
                                status = check_airtable_status(booking_ref)

                                if status and "no show" in status.lower():
                                    log("Airtable status confirmed: No Show.", "INFO")
                                    is_no_show = True
                                else:
                                    log("Not a No Show. Sending Webhook alert instead of auto-replying.", "INFO")
                                    send_negative_review_webhook(review_data)
                                    should_reply = False

                            if should_reply:
                                log("Initiating AI reply...", "AI_ACTION")
                                click_like_human(reply_btn)
                                wait_for_page_settle(page, 1.5, 3)

                                textarea = page.locator('textarea')

                                if textarea.count() > 0 and textarea.first.is_visible():
                                    customer_name = "Traveler"
                                    ai_reply = generate_reply(comment, rating, customer_name, is_no_show)

                                    if ai_reply:
                                        log("Typing reply into textarea...", "AI_ACTION")
                                        type_like_human(textarea, ai_reply)

                                        send_btn = page.locator('button:has-text("Send reply")').or_(page.locator('button:has-text("Send")'))

                                        try:
                                            send_btn.first.wait_for(state="visible", timeout=5000)
                                            if send_btn.first.is_disabled():
                                                log("Send button is disabled. Waiting...", "DEBUG")
                                                textarea.first.evaluate("el => el.dispatchEvent(new Event('input', {bubbles: true}))")
                                                time.sleep(1)
                                        except:
                                            pass

                                        if send_btn.count() > 0 and send_btn.first.is_visible() and not send_btn.first.is_disabled():
                                            click_like_human(send_btn)
                                            log("AI Reply sent successfully.", "SUCCESS")

                                            if booking_ref not in replied_cache:
                                                replied_cache.append(booking_ref)
                                                save_replied_cache(replied_cache)

                                            review_data["review_comment"] += f"\n\n--- AI Reply ---\n{ai_reply}"

                                            time.sleep(2)
                                        else:
                                            log("Send button not found.", "WARNING")
                                            page.keyboard.press("Escape")
                                    else:
                                        log("Failed to generate AI reply.", "WARNING")
                                        page.keyboard.press("Escape")
                                else:
                                    log("Reply popup/textarea not found.", "WARNING")
                    except Exception as e:
                        log(f"Error in AI Reply flow: {e}", "ERROR")
                        try:
                            page.keyboard.press("Escape")
                        except:
                            pass

                    review_hash = f"{booking_ref}_{rating}"
                    count = cache.get(review_hash, 0)

                    if count < MAX_REPEATS:
                        log(f"Syncing review for {booking_ref}...")
                        sync_to_airtable(review_data)
                        cache[review_hash] = count + 1
                        save_cache(cache)
                        processed_records += 1
                    else:
                        log(f"Skipping duplicate: {booking_ref}", "INFO")

                except Exception as e:
                    errors.append(f"review_{index}: {e}")
                    log(f"Error processing review {index}: {e}", "ERROR")
            
            has_next_page = False
            
            try:
                next_btn = page.locator('nav[role="navigation"] a:has-text("Next")').or_(page.locator('a[aria-label="Next page"]')).or_(page.locator('a:has-text(">")'))
                
                if next_btn.count() > 0 and next_btn.first.is_visible():
                    has_next_page = True
                else:
                    next_page_link = page.locator(f'nav a[href*="page={page_num + 1}"]')
                    if next_page_link.count() > 0:
                        has_next_page = True
            except:
                pass
            
            if len(review_cards) == 0:
                log("No reviews on this page. Stopping.", "INFO")
                break
                
            next_page_index = page_num + 1
            if next_page_index >= MAX_PAGES:
                log("Reached safety page limit.", "WARNING")
                break
            
            try:
                disabled_next = page.locator('button[disabled]:has-text("Next")').or_(page.locator('a[aria-disabled="true"]:has-text("Next")'))
                if disabled_next.count() > 0:
                    log("Next button is disabled. Reached last page.", "SUCCESS")
                    break
            except:
                pass

            log(f"Navigating to page {next_page_index + 1}...")
            next_url = build_reviews_url(next_page_index)
            
            navigate_with_delay(page, next_url, 3, 6)

            current_url = page.url.lower()
            if "login" in current_url or "signin" in current_url or has_login_form(page):
                context, page = recover_session_for_page(browser, context, page, owns_browser, is_headless, next_page_index, recreate_fresh_context)
                continue
            
            expected_next_url = build_reviews_url(next_page_index)
            current_url = page.url.rstrip("/")
            if current_url != expected_next_url.rstrip("/"):
                log(f"Redirected to unexpected URL ({page.url}) instead of {expected_next_url}. Assuming end of pagination.", "INFO")
                break
        log("Scraping completed.", "SUCCESS")
    except CycleRestartRequested as e:
        pending_restart = e
        log(e.reason, "ERROR")
    finally:
        if owns_browser:
            try:
                context.storage_state(path=STATE_FILE)
            except:
                pass

    if pending_restart:
        raise pending_restart

    return context, page, {
        "processed_records": processed_records,
        "errors": errors,
        "status": "success" if not errors else "success_with_errors"
    }

def scrape_cycle():
    """
    Backwards-compatible single-cycle entry point.
    Creates a temporary browser session and runs one cycle.
    """
    effective_mode = get_effective_browser_mode()
    is_headless = effective_mode == "headless"

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser, context, page, owns_browser, recreate_fresh_context = create_browser_session(p, is_headless)
        scrape_cycle_in_session(browser, context, page, owns_browser, recreate_fresh_context, is_headless)

def run_continuous_scraper():
    while True:
        log("=== Starting New Scrape Cycle ===", "CYCLE_START")
        delay_minutes = CYCLE_DELAY_MINUTES
        try:
            scrape_cycle()
        except CycleRestartRequested as e:
            delay_minutes = e.delay_minutes
            log(f"Cycle restart requested: {e.reason}", "WARNING")
        except Exception as e:
            log(f"Cycle crashed: {e}", "ERROR")
            
        sleep_sec = int(os.getenv("CYCLE_DELAY_SECONDS", str(delay_minutes * 60)))
        log(f"Cycle finished. Waiting {sleep_sec} seconds before restart...", "CYCLE_END")
        time.sleep(sleep_sec)

def write_account_report(account_index, report):
    profile_root = get_profile_root(account_index)
    reports_dir = profile_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    last_path = str((reports_dir / "last_report.json").resolve())
    history_path = str((reports_dir / "history.jsonl").resolve())
    payload = dict(report)
    payload["account_index"] = account_index
    payload["timestamp"] = datetime.now().isoformat()

    with _REPORT_WRITE_LOCK:
        atomic_write_json(last_path, payload)
        try:
            with open(history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except:
            pass

def run_account_worker(account_index, once=False):
    """
    Runs the scraper loop for a single account inside its own process.
    """
    configure_account_globals(account_index)
    effective_mode = get_effective_browser_mode()
    is_headless = effective_mode == "headless"
    start_msg = f"Account {account_index}: starting (persistent={PERSISTENT_PROFILE}, keep_open={KEEP_BROWSERS_OPEN})"
    log(start_msg, "START")

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        while True:
            try:
                browser, context, page, owns_browser, recreate_fresh_context = create_account_browser_session(p, is_headless, account_index)
                break
            except Exception as e:
                log(f"Account {account_index}: browser bootstrap failed: {e}", "ERROR")
                time.sleep(3)

        while True:
            cycle_started = time.time()
            processed = 0
            errors = []
            status = "success"
            delay_minutes = CYCLE_DELAY_MINUTES
            try:
                page = close_extra_tabs(context, keep_pages=1) or page
                context, page, stats = scrape_cycle_in_session(
                    browser, context, page, owns_browser, recreate_fresh_context, is_headless
                )
                processed = int(stats.get("processed_records", 0) or 0)
                errors = list(stats.get("errors") or [])
                status = str(stats.get("status") or "success")
            except CycleRestartRequested as e:
                status = "restart"
                delay_minutes = e.delay_minutes
                errors.append(str(e.reason))
                log(f"Cycle restart requested: {e.reason}", "WARNING")
            except Exception as e:
                status = "error"
                errors.append(str(e))
                log(f"Cycle crashed: {e}", "ERROR")

            elapsed = max(0.0, time.time() - cycle_started)
            write_account_report(account_index, {
                "status": status,
                "processed_records_estimate": processed,
                "errors": errors,
                "elapsed_seconds": elapsed,
                "airtable_table_name": AIRTABLE_TABLE_NAME
            })

            if once:
                break

            sleep_sec = int(os.getenv("CYCLE_DELAY_SECONDS", str(delay_minutes * 60)))
            log(f"Cycle finished. Waiting {sleep_sec} seconds before restart...", "CYCLE_END")
            time.sleep(sleep_sec)

        if KEEP_BROWSERS_OPEN:
            log("One-shot finished. Keeping browser open. Press Ctrl+C to exit this account worker.", "ACTION")
            while True:
                time.sleep(60)
        else:
            try:
                context.close()
            except:
                pass

def run_multi_account_supervisor(account_count, once=False):
    """
    Spawns one process per account. Each process owns its browser and files.
    A failure in one account does not stop the others.
    """
    procs = {}
    for idx in range(1, account_count + 1):
        p = mp.Process(target=run_account_worker, args=(idx, once), name=f"gyg_account_{idx}")
        p.start()
        procs[idx] = p

    if once:
        for proc in procs.values():
            proc.join()
        return

    while True:
        for idx, proc in list(procs.items()):
            if not proc.is_alive():
                exit_code = proc.exitcode
                log(f"Account {idx} worker exited (code={exit_code}). Restarting...", "WARNING")
                p = mp.Process(target=run_account_worker, args=(idx, once), name=f"gyg_account_{idx}")
                p.start()
                procs[idx] = p
        time.sleep(5)

if __name__ == "__main__":
    import sys
    once = "--once" in sys.argv
    if ACCOUNT_COUNT > 1:
        run_multi_account_supervisor(ACCOUNT_COUNT, once=once)
    else:
        run_account_worker(1, once=once)
