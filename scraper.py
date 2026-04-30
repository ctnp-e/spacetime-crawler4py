import re
import atexit
import hashlib
from collections import Counter
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup

# for report
from stop_words import get_stop_words

NUM_WORDS = 20
USEFUL_RATIO = 0.1  # minimum words-per-tag; below this = markup-heavy, low info

''' For duplicate/near-duplicate detection, we use a combination of:'''
seen_hashes = set()
seen_simhashes = []
# possibly too harsh.
SIMHASH_THRESHOLD = 6  # pages differing by <= 6 bits are near-duplicates

# what if we wanted to do...
# document D1 is a near-duplicate of document D2 if more than
# 90% of the words in the documents are the same
seen_minhash_sigs = []
NUM_HASHES = 128       # signature length — more = more accurate, slower
MINHASH_THRESHOLD = 0.9  # estimated Jaccard > 90% = near-duplicate
_BIG_PRIME = (1 << 61) - 1

'''analytic stuff'''
STOP_WORDS = set(get_stop_words('en'))

unique_pages = set()    # unique URLs seen (fragment-stripped)
longest_page = ("", 0)  # (url, word_count)
word_freq = Counter()   # word frequencies across all crawled pages
subdomains = {}         # netloc -> set of unique page URLs



'''
requirements to hit:
x Honor the politeness delay for each site
  Crawl all pages with high textual information content
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

'''
someone said :
how many pages did you have
mine just stopped at 5.5k 0_0
5.2k 

someone said :
70 subdomains is too little...
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

    # Track unique pages and subdomains by URL (per assignment definition),
    # before any content filtering
    page_url = urlunparse(urlparse(url)._replace(fragment=""))
    unique_pages.add(page_url)
    subdomains.setdefault(urlparse(page_url).netloc.lower(), set()).add(page_url)

    content_hash = hashlib.md5(resp.raw_response.content).hexdigest()
    if content_hash in seen_hashes:
        return []
    seen_hashes.add(content_hash)

    ''' Crawl all pages with high textual information content '''
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    for tag in soup(["script", "style", "header", "footer", "nav"]):
        tag.decompose()
    

    words = soup.get_text(separator=" ").split()

    # this is for finding the ratio of annoying empty page with lots of tags. 
    # lowkey it removed too many pages
    # tag_count = len(soup.find_all())
    # useful_ratio = len(words) / tag_count if tag_count > 0 else 0 

    # Low-content checks
    if len(words) < NUM_WORDS : # or useful_ratio < USEFUL_RATIO:
        return []

    # Near-duplicate check
    # only runs for pages that pass everything else!
    fingerprint = simhash(words)
    if any(hamming_distance(fingerprint, h) <= SIMHASH_THRESHOLD for h in seen_simhashes):
        return []
    seen_simhashes.append(fingerprint)

    
    # sig = minhash_signature(words)
    # if any(minhash_similarity(sig, s) > MINHASH_THRESHOLD for s in seen_minhash_sigs):
    #     return []
    # seen_minhash_sigs.append(sig)

    # Page passed all quality checks — update word stats
    global longest_page

    if len(words) > longest_page[1]:
        longest_page = (page_url, len(words))
    for w in words:
        w = w.lower()
        if w.isalpha() and w not in STOP_WORDS: # for report
            word_freq[w] += 1

    links = []
    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        link = urlunparse(urlparse(link)._replace(fragment=""))
        if is_valid(link):
            links.append(link)

    return links



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

        # calendar traps: /2024/01/ or /2024/01/15/ but not /cs121/2024/
        if re.search(r"/\d{4}/\d{1,2}(/|$)", parsed.path):
            return False
        # specficially!

        # What the new pattern correctly allows:
        # /cs121/2024/          - no digit segment after 2024/
        # /research/2024-goals/ - dash not slash between year and next part
        # /projects/h264/       - h264 is not 4 digits alone
        # /page/20241/          5 digits, not matched

        # What it correctly blocks:
        # /events/2024/01/      - /2024/01/ matches
        # /news/2023/12/25/     - /2023/12/ matches (catches the prefix)
        # /archive/2019/3/      - /2019/3/ matches
        
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

# MIN HASH HELPERS!!!!
def get_shingles(words, n=2):
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

def minhash_signature(words):
    shingles = get_shingles(words)
    if not shingles:
        return [0] * NUM_HASHES
    hashed = [int(hashlib.md5(s.encode()).hexdigest(), 16) for s in shingles]
    return [min((a * x + a) % _BIG_PRIME for x in hashed) for a in range(1, NUM_HASHES + 1)]

def minhash_similarity(sig1, sig2):
    return sum(a == b for a, b in zip(sig1, sig2)) / NUM_HASHES

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