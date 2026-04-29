import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

NUM_WORDS = 100
USEFUL_RATIO = 0.8  # minimum words-per-tag; below this = markup-heavy, low info



'''
requirements to hit:
? Honor the politeness delay for each site
  Crawl all pages with high textual information content
? Detect and avoid infinite traps
  Detect and avoid sets of similar pages with no information
x Detect and avoid dead URLs that return a 200 status but no data 
x Detect and avoid crawling very large files, especially if they have low information value
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


    if (not is_valid(url) or resp.status != 200):
        return list()
    
    links = []
    for link in re.findall(r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"', resp.raw_response.content.decode()):
        if is_valid(link):
            links.append(link)

    ''' Crawl all pages with high textual information content '''
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    for tag in soup(["script", "style", "header", "footer", "nav"]):
        tag.decompose()
    

    text = soup.get_text(separator=" ") # just the text form now
    words = text.split()                # split into words
    tag_count = len(soup.find_all())
    useful_ratio = len(words) / tag_count if tag_count > 0 else 0

    # Low-content checks
    if len(words) < NUM_WORDS or useful_ratio < USEFUL_RATIO: # too few words, likely not a useful page
        ''' also helps clear "return a 200 status but no data" '''
        print("USELESS!")
        return []

    links = []
    for tag in soup.find_all("a", href=True):
        link = tag["href"]
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
