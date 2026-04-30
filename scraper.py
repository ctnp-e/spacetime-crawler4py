import re
import hashlib
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup

NUM_WORDS = 100
USEFUL_RATIO = 0.8  # minimum words-per-tag; below this = markup-heavy, low info

seen_hashes = set()
seen_simhashes = []
SIMHASH_THRESHOLD = 3  # pages differing by <= 3 bits are near-duplicates



'''
requirements to hit:
? Honor the politeness delay for each site
  Crawl all pages with high textual information content
? Detect and avoid infinite traps
  Detect and avoid sets of similar pages with no information
x   - no duplicate pages (e.g. by content hash)
!   - no near-duplicate pages (e.g. by content similarity)
x Detect and avoid dead URLs that return a 200 status but no data 
x Detect and avoid crawling very large files, especially if they have low information value

TODO:
NEAR DUPLICATES!!!
stop_words?
local storage instead?!
implement multithreading?
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


    if not is_valid(url) or resp.status != 200 or not resp.raw_response:
        return []

    content_hash = hashlib.md5(resp.raw_response.content).hexdigest()
    if content_hash in seen_hashes:
        return []
    seen_hashes.add(content_hash)

    ''' Crawl all pages with high textual information content '''
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    for tag in soup(["script", "style", "header", "footer", "nav"]):
        tag.decompose()
    

    words = soup.get_text(separator=" ").split()
    tag_count = len(soup.find_all())
    useful_ratio = len(words) / tag_count if tag_count > 0 else 0

    # Low-content checks
    if len(words) < NUM_WORDS or useful_ratio < USEFUL_RATIO:
        return []

    # Near-duplicate check
    # only runs for pages that pass everything else!
    fingerprint = simhash(words)
    if any(hamming_distance(fingerprint, h) <= SIMHASH_THRESHOLD for h in seen_simhashes):
        return []
    seen_simhashes.append(fingerprint)

    links = []
    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        link = urlunparse(urlparse(link)._replace(fragment=""))
        if is_valid(link):
            links.append(link)

    return links


def simhash(words):
    '''
    hashes each word
    accumulates a weighted bit vector across all 64 bit positions
    produces a single 64-bit integer fingerprint
    '''
    v = [0] * 64
    for word in words:
        h = int(hashlib.md5(word.encode()).hexdigest(), 16) & ((1 << 64) - 1)
        for i in range(64):
            v[i] += 1 if h & (1 << i) else -1
    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1 << i)
    return fingerprint

def hamming_distance(h1, h2):
    ''' 
    counts how many bits differ between two fingerprints
    pages with very similar text will differ by only a few bits
    '''
    return bin(h1 ^ h2).count('1')

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
        
        # path depth is too deep
        if parsed.path.count("/") > 10:
            return False

        # repeating path segments trap
        segments = [s for s in parsed.path.split("/") if s]
        if len(segments) != len(set(segments)):
            return False

        # calender traps
        if re.search(r"\d{4}[-/]\d{1,2}([-/]\d{1,2})?", parsed.path):
            return False
        
        # too long of a url - basic trap
        if len(url) > 200:
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
