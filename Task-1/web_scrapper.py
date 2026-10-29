import csv
import json
import logging
import os
import random
import sys
import threading
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib import robotparser
from urllib.parse import parse_qsl, urlencode, urldefrag, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# =============
# CONFIG  
# =============

MAX_PAGES_PER_SOURCE = 2000        # target ARTICLES saved per source (100 sources -> ~100,000)
MAX_REQUESTS_PER_SOURCE = 6000     # safety cap on pages fetched per source (many pages are not articles)
DELAY_RANGE = (1, 3)               # random polite delay (seconds) between requests to the same source
TIMEOUT = 15                       # seconds, applied to EVERY request
WORKERS = 15                       # parallel threads (one source per thread)
MAX_RETRIES = 3                    # attempts per URL before giving up on it
BACKOFF_BASE = 2                   # wait 2s, 4s, 8s ... between retries (exponential backoff)
MAX_CONSECUTIVE_FAILURES = 10      # skip the source if this many pages in a row fail
MAX_BLOCKS = 8                     # skip the source after this many 403/429 responses
MIN_ARTICLE_CHARS = 600            # minimum clean text length for a page to count as an article
USE_PLAYWRIGHT = True              # fall back to a headless browser for JS-rendered/empty pages
JS_FALLBACK_MIN_CHARS = 300        # if requests returns less visible text than this, try Playwright

OUTPUT_FILE = "articles.csv"
VISITED_FILE = "visited_urls.txt"
FRONTIER_DIR = "frontier"          # unfinished queues are saved here (needed for proper resume)
LOG_FILE = "crawler.log"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 ResearchCrawler/1.0 "
    "(contact: your-email@example.com)"   # <-- put a real contact address here
)

# ===========
# SOURCES  
# ===========

SOURCES = [
    # ---------- Tech news (15) ----------
    "https://techcrunch.com",
    "https://arstechnica.com",
    "https://www.engadget.com",
    "https://hackaday.com",
    "https://venturebeat.com",
    "https://www.zdnet.com",
    "https://www.techradar.com",
    "https://www.theverge.com",
    "https://slashdot.org",
    "https://www.technologyreview.com",
    "https://www.infoq.com",
    "https://thenewstack.io",
    "https://www.androidauthority.com",
    "https://9to5mac.com",
    "https://www.xda-developers.com",

    # ---------- Programming / AI / data blogs (12) ----------
    "https://dev.to",
    "https://www.freecodecamp.org/news",
    "https://realpython.com",
    "https://www.kdnuggets.com",
    "https://machinelearningmastery.com",
    "https://www.analyticsvidhya.com/blog",
    "https://www.smashingmagazine.com",
    "https://css-tricks.com",
    "https://alistapart.com",
    "https://hackernoon.com",
    "https://www.marktechpost.com",
    "https://www.baeldung.com",

    # ---------- Science / health (8) ----------
    "https://www.sciencedaily.com",
    "https://phys.org",
    "https://www.livescience.com",
    "https://www.space.com",
    "https://www.sciencenews.org",
    "https://www.medicalnewstoday.com",
    "https://www.popsci.com",
    "https://theconversation.com",

    # ---------- World / regional news (14) ----------
    "https://www.theguardian.com/international",
    "https://www.npr.org",
    "https://www.aljazeera.com",
    "https://www.dw.com/en",
    "https://www.france24.com/en",
    "https://www.dawn.com",
    "https://tribune.com.pk",
    "https://www.thenews.com.pk",
    "https://www.thehindu.com",
    "https://www.straitstimes.com",
    "https://www.abc.net.au/news",
    "https://www.cbc.ca/news",
    "https://www.japantimes.co.jp",
    "https://globalvoices.org",

    # ---------- Government / international organizations (8) ----------
    "https://news.un.org/en",
    "https://www.who.int/news",
    "https://www.worldbank.org/en/news",
    "https://www.nasa.gov",
    "https://www.esa.int",
    "https://www.nih.gov/news-events",
    "https://www.noaa.gov/news",
    "https://www.usgs.gov/news",

    # ---------- Others (3) ----------
    "https://www.propublica.org",
    "https://lifehacker.com",
    "https://www.howtogeek.com",
]

# URL filters: skip files that are not HTML pages and pages that are never articles
SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp",
    ".pdf", ".zip", ".rar", ".gz", ".tar", ".7z", ".exe", ".dmg", ".apk",
    ".mp3", ".mp4", ".avi", ".mov", ".wav", ".css", ".js", ".json", ".xml",
    ".rss", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv",
}
SKIP_KEYWORDS = (
    "/login", "/signin", "/sign-in", "/signup", "/sign-up", "/register",
    "/cart", "/checkout", "/account", "/wp-admin", "/wp-login", "/cdn-cgi/",
    "/feed", "/subscribe", "/unsubscribe", "/print/", "/share?",
)

CSV_COLUMNS = ["title", "published_date", "author", "link", "content"]

# =========================
# GLOBAL SHARED STATE
# =========================

CSV_LOCK = threading.Lock()        # only one thread writes to the CSV at a time
VISITED_LOCK = threading.Lock()    # protects visited set + visited file
VISITED = set()                    # every URL already fetched (or disallowed) in this or earlier runs
STOP = threading.Event()           # set on Ctrl+C so all threads finish cleanly
ERROR_COUNT = 0                    # total errors, shown in the progress bar

log = logging.getLogger("crawler")


# ============================
# LOGGING / SMALL HELPERS
# ============================

class TqdmHandler(logging.Handler):
    """Print log lines through tqdm.write so they don't break the progress bar."""

    def emit(self, record):
        try:
            tqdm.write(self.format(record))
        except Exception:
            pass


def setup_logging():
    """Log everything to a file, and only warnings/errors to the console."""
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(threadName)s] %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    console = TqdmHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(fmt)
    log.addHandler(file_handler)
    log.addHandler(console)


def count_error():
    """Increase the global error counter (shown in the progress bar)."""
    global ERROR_COUNT
    ERROR_COUNT += 1


def get_domain(url):
    """Return the host of a URL without 'www.' so www.x.com and x.com count as the same site."""
    host = urlsplit(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


# =============
# URL HANDLING
# =============

def normalize_url(base_url, href):
    """
    Turn a raw href into a clean absolute URL.
    WHY: the same page can appear as relative links, with #fragments or tracking
    params; normalising makes the 'visited' check reliable.
    """
    if not href:
        return None
    href = href.strip()
    if href.lower().startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
        return None
    absolute = urljoin(base_url, href)            # handle relative links
    absolute, _fragment = urldefrag(absolute)     # remove #fragment
    parts = urlsplit(absolute)
    if parts.scheme not in ("http", "https"):
        return None
    # Drop tracking parameters so ?utm_source=x doesn't create "new" URLs
    query = [
        (k, v) for k, v in parse_qsl(parts.query)
        if not k.lower().startswith("utm_") and k.lower() not in ("fbclid", "gclid", "ref")
    ]
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path or "/", urlencode(query), ""))


def should_skip_url(url):
    """True for non-HTML files (images, pdf, zip...) and pages like login/cart."""
    path = urlsplit(url).path.lower()
    if os.path.splitext(path)[1] in SKIP_EXTENSIONS:
        return True
    return any(word in path for word in SKIP_KEYWORDS)


# =========================================
# VISITED URLS + FRONTIER (resume support)
# =========================================

def load_visited():
    """Load URLs scraped in earlier runs so we never fetch them twice."""
    if os.path.exists(VISITED_FILE):
        with open(VISITED_FILE, "r", encoding="utf-8") as f:
            VISITED.update(line.strip() for line in f if line.strip())
    log.info("Loaded %d visited URLs", len(VISITED))


def mark_visited(url):
    """Remember a URL in memory AND append it to disk immediately (crash-safe)."""
    with VISITED_LOCK:
        if url in VISITED:
            return
        VISITED.add(url)
        with open(VISITED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")


def frontier_path(domain):
    """File where a source's unfinished queue is stored."""
    return os.path.join(FRONTIER_DIR, domain.replace(":", "_") + ".txt")


def load_frontier(domain):
    """Load the saved queue for a source (empty deque if this is the first run)."""
    path = frontier_path(domain)
    if not os.path.exists(path):
        return deque()
    with open(path, "r", encoding="utf-8") as f:
        return deque(line.strip() for line in f if line.strip())


def save_frontier(domain, queue):
    """Save the unfinished queue so the next run continues exactly where we stopped."""
    os.makedirs(FRONTIER_DIR, exist_ok=True)
    with open(frontier_path(domain), "w", encoding="utf-8") as f:
        f.write("\n".join(queue))


# ==========
# CSV
# ==========

def save_to_csv(row):
    """Append one article row to the CSV. The lock stops threads mixing up lines."""
    with CSV_LOCK:
        is_new = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if is_new:
                writer.writeheader()
            writer.writerow(row)


def load_existing_counts():
    """Count articles already in the CSV per domain, so a resumed run only fetches the remainder."""
    counts = Counter()
    if not os.path.exists(OUTPUT_FILE):
        return counts
    csv.field_size_limit(2**31 - 1)  # article text can exceed the default csv field limit
    with open(OUTPUT_FILE, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            counts[get_domain(row.get("link", ""))] += 1
    return counts


# ===================
# PER-SOURCE STATE
# ==================

@dataclass
class SourceState:
    """Everything one crawler thread needs to know about its source."""
    seed: str
    domain: str
    session: requests.Session
    robots: robotparser.RobotFileParser
    queue: deque = field(default_factory=deque)
    queued: set = field(default_factory=set)   # URLs ever put in the queue (avoids duplicates)
    saved: int = 0                             # articles saved for this source (incl. earlier runs)
    requests_made: int = 0
    consecutive_failures: int = 0
    blocks: int = 0                            # number of 403/429 answers
    browser: object = None                     # lazy BrowserFetcher (Playwright)
    crawl_delay: float = 0


# ============
# ROBOTS.TXT
# ============

def load_robots(session, seed):
    """
    Download and parse robots.txt for the source.
    WHY requests instead of rp.read(): we need a timeout and our own User-Agent.
    Rule used: robots.txt missing/unreachable (4xx or error) => no restrictions.
    """
    parts = urlsplit(seed)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        resp = session.get(robots_url, timeout=TIMEOUT)
        rp.parse(resp.text.splitlines() if resp.status_code == 200 else [])
    except requests.RequestException as exc:
        log.warning("robots.txt not reachable for %s (%s) - assuming allowed", parts.netloc, exc)
        rp.parse([])
    return rp


# =============
# FETCHING
# =============

class BrowserFetcher:
    """Headless-Chromium helper (Playwright). Created lazily, one per source thread,
    because Playwright's sync API must not be shared between threads."""

    def __init__(self):
        self.pw = None
        self.browser = None
        self.context = None

    def get_html(self, url):
        """Load the page with JavaScript executed and return its HTML as bytes."""
        if self.browser is None:
            from playwright.sync_api import sync_playwright  # imported here so it stays optional
            self.pw = sync_playwright().start()
            self.browser = self.pw.chromium.launch(headless=True)
            self.context = self.browser.new_context(user_agent=USER_AGENT)
        page = self.context.new_page()
        try:
            page.goto(url, timeout=TIMEOUT * 2000, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)  # give client-side JS a moment to render content
            return page.content().encode("utf-8")
        finally:
            page.close()

    def close(self):
        """Shut the browser down (always called when the source finishes)."""
        try:
            if self.browser:
                self.browser.close()
            if self.pw:
                self.pw.stop()
        except Exception:
            pass


def make_session():
    """Create a requests.Session with a proper User-Agent (re-uses connections = faster)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def backoff_wait(attempt, retry_after=None):
    """Sleep before a retry: 2s, 4s, 8s... (+ jitter) or the server's Retry-After value."""
    if retry_after is not None:
        wait = min(retry_after, 60)
    else:
        wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
    STOP.wait(wait)  # like sleep(), but wakes up immediately on Ctrl+C


def fetch_page(state, url):
    """
    Download one URL. Returns (html_bytes, final_url) or None.
    Handles: 403/404/429/500, connection errors, timeouts, SSL errors,
    redirects (to other domains), empty pages. Retries up to MAX_RETRIES times.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        if STOP.is_set():
            return None
        try:
            resp = state.session.get(url, timeout=TIMEOUT, allow_redirects=True)
        except requests.exceptions.SSLError as exc:
            # Broken certificate: retrying will not help, and we never disable verification.
            log.warning("SSL error %s: %s", url, exc)
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            # Temporary network problems: wait and try again.
            log.info("Network error (%d/%d) %s: %s", attempt, MAX_RETRIES, url, type(exc).__name__)
            if attempt < MAX_RETRIES:
                backoff_wait(attempt)
            continue
        except requests.RequestException as exc:
            # Anything else (invalid URL, too many redirects, ...): give up on this URL.
            log.info("Request failed %s: %s", url, exc)
            break

        status = resp.status_code

        if status == 200:
            # Redirect check: after redirects we must still be on the same site.
            if get_domain(resp.url) != state.domain:
                log.info("Redirected off-domain, skipping: %s -> %s", url, resp.url)
                return None
            # Only HTML pages are interesting.
            ctype = resp.headers.get("Content-Type", "").lower()
            if ctype and "html" not in ctype:
                return None
            # Empty page: keep the (tiny) body anyway; the caller may try Playwright.
            state.consecutive_failures = 0
            return resp.content, resp.url          # raw bytes: BeautifulSoup detects encoding itself

        if status in (404, 410):
            return None                            # page is gone: not an error for the source
        if status == 403:
            state.blocks += 1                      # we are probably blocked: do not hammer
            log.warning("403 Forbidden on %s", url)
            break
        if status == 429:
            state.blocks += 1                      # rate limited: obey Retry-After if present
            retry_after = resp.headers.get("Retry-After", "")
            wait = float(retry_after) if retry_after.isdigit() else None
            log.warning("429 Too Many Requests on %s", url)
            if attempt < MAX_RETRIES:
                backoff_wait(attempt + 2, wait)    # extra patience for rate limits
            continue
        if status >= 500:
            log.info("Server error %d (%d/%d) %s", status, attempt, MAX_RETRIES, url)
            if attempt < MAX_RETRIES:
                backoff_wait(attempt)
            continue
        break                                      # other 3xx/4xx codes: skip

    state.consecutive_failures += 1
    count_error()
    return None


def build_soup(state, html, url):
    """
    Parse HTML with lxml. If the page is an empty JavaScript shell and Playwright is
    enabled, re-load it in a real browser. WHY: many modern sites render content with JS.
    """
    soup = BeautifulSoup(html, "lxml")
    visible_chars = len(soup.get_text(strip=True)) if soup.body else 0
    if USE_PLAYWRIGHT and visible_chars < JS_FALLBACK_MIN_CHARS:
        try:
            if state.browser is None:
                state.browser = BrowserFetcher()
            html = state.browser.get_html(url)
            soup = BeautifulSoup(html, "lxml")
        except ImportError:
            log.warning("Playwright not installed - disabling browser fallback")
            globals()["USE_PLAYWRIGHT"] = False
        except Exception as exc:
            log.info("Playwright failed for %s: %s", url, exc)
    return soup


# ==============
# EXTRACTION
# ==============
def read_json_ld(soup):
    """Return all JSON-LD objects on the page (many news sites put date/author there)."""
    items, stack = [], []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            stack.append(json.loads(tag.string or ""))
        except (ValueError, TypeError):
            continue
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            items.append(node)
            if "@graph" in node:
                stack.append(node["@graph"])
    return items


def author_to_text(value):
    """Turn a JSON-LD author value (str / dict / list) into a plain string."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(value.get("name", "")).strip()
    if isinstance(value, list):
        return ", ".join(filter(None, (author_to_text(v) for v in value)))
    return ""


def meta_content(soup, *pairs):
    """Return the first non-empty <meta> content for the given (attribute, value) pairs."""
    for attr, value in pairs:
        tag = soup.find("meta", attrs={attr: value})
        if tag and tag.get("content", "").strip():
            return tag["content"].strip()
    return ""


def extract_metadata(soup):
    """
    Get title, published date and author. Tries several common conventions
    (Open Graph, schema.org, <time>, JSON-LD) because every site does it differently.
    """
    json_ld = read_json_ld(soup)

    # ---- title ----
    title = meta_content(soup, ("property", "og:title"), ("name", "twitter:title"))
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
    if not title and soup.title:
        title = soup.title.get_text(strip=True)

    # ---- published date ----
    date = meta_content(
        soup, ("property", "article:published_time"), ("itemprop", "datePublished"),
        ("name", "date"), ("name", "pubdate"), ("name", "publish-date"),
        ("property", "og:article:published_time"),
    )
    if not date:
        for node in json_ld:
            if node.get("datePublished"):
                date = str(node["datePublished"])
                break
    if not date:
        time_tag = soup.find("time")
        if time_tag:
            date = time_tag.get("datetime") or time_tag.get_text(strip=True)

    # ---- author ----
    author = meta_content(soup, ("name", "author"), ("property", "article:author"),
                          ("name", "parsely-author"), ("name", "twitter:creator"))
    if not author:
        for node in json_ld:
            if node.get("author"):
                author = author_to_text(node["author"])
                break
    if not author:
        tag = soup.find(attrs={"rel": "author"}) or soup.find(attrs={"itemprop": "author"})
        if tag:
            author = tag.get_text(" ", strip=True)

    return {"title": title.strip(), "published_date": date.strip(), "author": author.strip()}


def get_links(soup, page_url):
    """Collect every normalised <a href> link on the page (used to continue the BFS)."""
    links = set()
    for a in soup.find_all("a", href=True):
        link = normalize_url(page_url, a["href"])
        if link:
            links.add(link)
    return links


def extract_content(soup):
    """
    Return clean article text. Removes menus/ads/scripts first, then joins the
    paragraphs of the <article> (or <main>/<body>) element.
    NOTE: this edits the soup, so call get_links/extract_metadata BEFORE it.
    """
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header",
                     "aside", "form", "iframe", "svg", "button"]):
        tag.decompose()
    container = soup.find("article") or soup.find("main") or soup.body or soup
    paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    paragraphs = [p for p in paragraphs if len(p) >= 40]   # drop captions / "Share" lines
    return "\n".join(paragraphs).strip()


def is_article_page(soup, text):
    """
    Decide if a page is a real article: enough text AND a structural hint
    (<article> tag or og:type=article). Keeps category/index pages out of the CSV.
    """
    if len(text) < MIN_ARTICLE_CHARS:
        return False
    has_article_tag = soup.find("article") is not None
    og_type = meta_content(soup, ("property", "og:type")).lower()
    return has_article_tag or og_type == "article"


# ================
# CRAWLER
# ================

def polite_delay(state):
    """Wait a random 1-3 s (or the site's Crawl-delay if larger) so we don't overload the server."""
    delay = max(random.uniform(*DELAY_RANGE), state.crawl_delay)
    STOP.wait(delay)


def should_abort_source(state):
    """True when the source keeps failing or blocking us -> skip it and move on."""
    if state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
        log.warning("[%s] too many consecutive failures - skipping source", state.domain)
        return True
    if state.blocks >= MAX_BLOCKS:
        log.warning("[%s] blocked (403/429 x%d) - skipping source", state.domain, state.blocks)
        return True
    return False


def enqueue_links(state, links):
    """Add new same-domain, crawlable links to the BFS queue."""
    for link in links:
        if (link in VISITED or link in state.queued
                or get_domain(link) != state.domain      # same domain only
                or should_skip_url(link)):
            continue
        state.queued.add(link)
        state.queue.append(link)


def process_page(state, url, progress):
    """Fetch one URL, queue its links, and save it if it is an article."""
    result = fetch_page(state, url)
    state.requests_made += 1
    mark_visited(url)
    if result is None:
        return
    html, final_url = result
    if not html or not html.strip():
        log.info("Empty page: %s", url)
        return

    soup = build_soup(state, html, final_url)
    links = get_links(soup, final_url)           # must come before extract_content (it deletes tags)
    meta = extract_metadata(soup)
    text = extract_content(soup)
    enqueue_links(state, links)

    if is_article_page(soup, text):
        save_to_csv({**meta, "link": final_url, "content": text})
        state.saved += 1
        progress.update(1)
        progress.set_postfix(errors=ERROR_COUNT)
        if state.saved % 100 == 0:
            log.info("[%s] %d articles saved", state.domain, state.saved)


def crawl_source(seed, already_saved, progress):
    """
    Crawl ONE source with BFS until it has MAX_PAGES_PER_SOURCE articles, the request cap
    is hit, the queue is empty, or the source blocks us. Runs inside its own thread.
    """
    domain = get_domain(seed)
    state = None
    try:
        session = make_session()
        robots = load_robots(session, seed)
        state = SourceState(seed=seed, domain=domain, session=session, robots=robots,
                            saved=already_saved)
        state.crawl_delay = float(robots.crawl_delay(USER_AGENT) or robots.crawl_delay("*") or 0)

        # Resume: continue from the saved queue; otherwise start from the homepage seed.
        state.queue = load_frontier(domain)
        if not state.queue:
            VISITED.discard(seed)                  # allow re-reading the homepage to find new links
            state.queue.append(seed)
        state.queued = set(state.queue)

        log.info("[%s] start (already saved: %d, queue: %d)", domain, state.saved, len(state.queue))

        # ---- main BFS loop ----
        while (state.queue and state.saved < MAX_PAGES_PER_SOURCE
               and state.requests_made < MAX_REQUESTS_PER_SOURCE and not STOP.is_set()):
            if should_abort_source(state):
                break
            url = state.queue.popleft()
            if url in VISITED:
                continue
            if not state.robots.can_fetch(USER_AGENT, url):   # respect robots.txt
                mark_visited(url)
                continue
            polite_delay(state)
            try:
                process_page(state, url, progress)
            except Exception as exc:               # one bad page must never kill the source
                count_error()
                log.exception("[%s] error on %s: %s", domain, url, exc)
    except Exception as exc:                       # one bad source must never kill the run
        count_error()
        log.exception("[%s] source failed: %s", domain, exc)
    finally:
        if state is not None:
            save_frontier(domain, state.queue)     # remember where we stopped
            if state.browser:
                state.browser.close()
            log.info("[%s] finished: %d articles, %d requests", domain, state.saved, state.requests_made)
    return domain


# ===========
# MAIN
# ===========

def main():
    """Load saved state, start one thread per source, and wait for everyone to finish."""
    setup_logging()
    sources = list(dict.fromkeys(SOURCES))         # remove duplicates, keep order
    if not sources:
        print("SOURCES is empty - add some URLs first.")
        return

    load_visited()
    counts = load_existing_counts()
    total_target = len(sources) * MAX_PAGES_PER_SOURCE
    progress = tqdm(total=total_target, initial=sum(counts.values()), unit="article",
                    desc="Articles scraped")

    executor = ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="src")
    futures = {executor.submit(crawl_source, s, counts[get_domain(s)], progress): s for s in sources}
    try:
        for future in as_completed(futures):
            try:
                domain = future.result()
                tqdm.write(f"Done: {domain}")
            except Exception as exc:               # safety net; crawl_source already catches errors
                log.error("Worker crashed for %s: %s", futures[future], exc)
    except KeyboardInterrupt:
        # Ctrl+C: ask every thread to stop; they save their queues before exiting.
        tqdm.write("Stopping... saving state (this can take a few seconds)")
        STOP.set()
        for f in futures:
            f.cancel()
    finally:
        executor.shutdown(wait=True)
        progress.close()
        print(f"Finished. Rows are in {OUTPUT_FILE}, visited URLs in {VISITED_FILE}, log in {LOG_FILE}")


if __name__ == "__main__":
    main()