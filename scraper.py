import re
import atexit
import hashlib
from collections import Counter
from time import strftime
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs
from bs4 import BeautifulSoup

# for report
from stop_words import get_stop_words

NUM_WORDS = 20
USEFUL_RATIO = 0.1  # minimum words-per-tag; below this = markup-heavy, low info



'''analytic stuff'''
STOP_WORDS = set(get_stop_words('en'))

unique_pages = set()    # unique URLs seen (fragment-stripped)
longest_page = ("", 0)  # (url, word_count)
word_freq = Counter()   # word frequencies across all crawled pages
subdomains = {}         # netloc -> set of unique page URLs

DEBUG = True


open("crawl_log.txt", "w").close()  # clear log on each run

_log_buffer = []

def _flush_crawl_log():
    if not _log_buffer:
        return
    with open("crawl_log.txt", "a", encoding="utf-8") as f:
        f.writelines(_log_buffer)
    _log_buffer.clear()

atexit.register(_flush_crawl_log)


'''
requirements to hit:
x Honor the politeness delay for each site
x Crawl all pages with high textual information content
? Detect and avoid infinite traps
? Detect and avoid sets of similar pages with no information
x   - no duplicate pages (e.g. by content hash)
x   - no near-duplicate pages (e.g. by content similarity)
x Detect and avoid dead URLs that return a 200 status but no data 
x Detect and avoid crawling very large files, especially if they have low information value

TODO:
x NEAR DUPLICATES!!!
x stop_words?
  local storage instead?!
  implement multithreading?

x calculate the most common words
  find longest page
  find total numsubdomains 
       Submit the list of subdomains ordered alphabetically and the number of unique pages detected in each subdomain. The content of this list should be lines containing subdomain, number, for example:
       vision.ics.uci.edu, 100
x how many unique pages
'''

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    ''' Filter by content quality, parse and return links '''
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # Validation guards up front, so we only parse HTML once we know we want it.
    if not is_valid(url) or resp.status != 200 or not resp.raw_response:
        return []

    headers = getattr(resp.raw_response, "headers", {}) or {}
    content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
    mime = content_type.split(";", 1)[0].strip().lower()
    if mime and not (mime.startswith("text/")
                     or mime in {"application/xhtml+xml", "application/xml", "text/xml"}):
        return []

    # Parse once — share the soup between link extraction and text extraction.
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    # don't want to lose links living inside tags
    links = get_links(url, resp, soup=soup)

    text = take_text(url, resp, soup=soup)

    if not text:
        return []

    words = text.split()

    # Low-content check — skip pages with very little real text.
    if len(words) < NUM_WORDS:
        return []

    # it is now confirmed unqiue
    global longest_page
    page_url = urlunparse(urlparse(url)._replace(fragment=""))
    unique_pages.add(page_url)
    subdomains.setdefault(urlparse(page_url).netloc.lower(), set()).add(page_url)

    if len(words) > longest_page[1]:
        longest_page = (page_url, len(words))

    for w in words:
        w = w.lower()
        if w.isalpha() and w not in STOP_WORDS:
            word_freq[w] += 1

    return links

def get_links(url, resp, soup=None):
    ''' Extract and return all hyperlinks from the page content.
        Pass an existing soup to avoid re-parsing. '''
    if soup is None:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    all_hrefs = soup.find_all("a", href=True)
    links = []
    for tag in all_hrefs:
        link = urljoin(url, tag["href"])
        link = urlunparse(urlparse(link)._replace(fragment=""))
        links.append(link)
    return links

# Pure text extraction, callable from Worker for similarity
# checking before scraping happens. Pass an existing soup to skip re-parsing
# (extract_next_links does this).
def take_text(url, resp, soup=None):
    if soup is None:
        # Standalone path (FOR WOKRER))
        if not is_valid(url) or resp.status != 200 or not resp.raw_response:
            return None

        # Skip non-HTML responses (PDFs, images, JSON, binary blobs etc).
        # Some endpoints serve binary content with no extension hint in the URL,
        # so the extension check in is_valid can't catch them

        headers = getattr(resp.raw_response, "headers", {}) or {}
        content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
        mime = content_type.split(";", 1)[0].strip().lower()
        if mime and not (mime.startswith("text/")
                         or mime in {"application/xhtml+xml", "application/xml", "text/xml"}):
            return None

        soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    
    # should call get_links BEFORE take_text
    for tag in soup(["script", "style", "header", "footer", 
                     "nav", "aside","form", "noscript", 
                     "button", "iframe"]):
        tag.decompose()

    full_text = soup.get_text(separator=" ")
    return full_text

def is_valid(url):
    ''' Filter by URL pattern (domain, file extension, scheme) '''
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False
        if not re.match(
            r"^(.+\.)?(ics|cs|informatics|stat)\.uci\.edu$", parsed.netloc.lower()):
            return False
        
        if (is_trap(url)):
            return False

        # too long of a url - basic trap
        if len(url) > 1000:
            return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def is_trap(url):
    parsed = urlparse(url)
    path_lower = parsed.path.lower()

    # path depth is too deep
    if parsed.path.count("/") > 10:
        return True

    # repeating path segments trap
    segments = [s for s in parsed.path.split("/") if s]
    if len(segments) != len(set(segments)):
        return True

    # calendar traps: /2024/01/ or /2024/01/15/ but not /cs121/2024/
    if re.search(r"/\d{4}/\d{1,2}(/|$)", path_lower):
        return True

    # ISO-date traps: /events/2024-01-15/, /posts/2024-01/, etc.
    if re.search(r"/\d{4}-\d{1,2}(-\d{1,2})?(/|$)", path_lower):
        return True

    # calendar archive views
    if re.search(r"/events/(month|list|today)(/|$)", path_lower):
        return True
    if re.search(r"/events/[^/]+/day/\d{4}-\d{2}-\d{2}", path_lower):
        return True

    query = parse_qs(parsed.query)

    # doku is INFINITE CONTENT it is INSANE
    if "/doku.php" in path_lower:
        do_vals = {v.lower() for v in query.get("do", [])}
        if do_vals & {"edit", "diff", "index", "recent",
                      "backlink", "revisions", "media"}:
            return True
        if {"rev", "rev2", "difftype", "tab_files", "tab_details"} & query.keys():
            return True

    # doku endpoints
    if re.search(r"/lib/exe/(fetch|detail)\.php", path_lower):
        return True

    # tracking / session params — same page reached under many URLs
    tracking = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                "sid", "sessionid", "phpsessid", "jsessionid", "fbclid", "gclid",
                "ref"}
    if any(k.lower() in tracking for k in query):
        return True

    # repeated query keys (?page=1&page=2) usually means a pagination loop
    if any(len(v) > 1 for v in query.values()):
        return True

    # goodbye all images
    image_types = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".ico", ".tiff")
    for values in query.values():
        for value in values:
            if value.lower().endswith(image_types):
                return True

    # too many pages likely pagination trap, e.g. ?page=1, ?page=2, ... ?page=10000
    for key, vals in query.items():
        if key.lower() in {"page", "p", "start", "offset"}:
            for v in vals:
                if v.isdigit() and int(v) > 1000:
                    return True

    return False



# For report
def generate_report(path="report.txt"):
    global longest_page
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"1. Unique pages found: {len(unique_pages)}\n\n")

        f.write(f"2. Longest page: {longest_page[0]}\n")
        f.write(f"   Word count: {longest_page[1]}\n\n")

        f.write("3. Top 50 most common words:\n")
        for rank, (word, count) in enumerate(word_freq.most_common(50), 1):
            f.write(f"   {rank:2}. {word} ({count})\n")
        f.write("\n")

        f.write("4. Subdomains (alphabetical):\n")
        for netloc in sorted(subdomains):
            f.write(f"   {netloc}, {len(subdomains[netloc])}\n")
    print(f"[report] Written to {path}")

atexit.register(generate_report)