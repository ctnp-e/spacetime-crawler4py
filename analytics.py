"""Analytics state + report writer for the crawler.

Extracted from scraper.py so that scraper.py is parsing/URL-filtering only.
The worker calls record_page() after the similarity check; everything else
(periodic on-disk snapshot, atexit final report) is wired up here.
"""

import atexit
import threading
from collections import Counter
from urllib.parse import urlparse, urlunparse

from stop_words import get_stop_words


NUM_WORDS = 20

STOP_WORDS = set(get_stop_words('en'))

unique_pages = set()    # unique URLs seen (fragment-stripped)
longest_page = ("", 0)  # (url, word_count)
word_freq = Counter()   # word frequencies across all crawled pages
subdomains = {}         # netloc -> count of unique pages (int, not set of URLs)
_stats_lock = threading.Lock()

# Periodic snapshot of the report so SIGKILL/OOM doesn't wipe out the run
_CHECKPOINT_EVERY = 50
_checkpoint_counter = 0
_checkpoint_lock = threading.Lock()


def _checkpoint_if_due():
    global _checkpoint_counter
    with _checkpoint_lock:
        _checkpoint_counter += 1
        if _checkpoint_counter % _CHECKPOINT_EVERY != 0:
            return
    # Drop count==1 long-tail entries 
    with _stats_lock:
        stale = [w for w, c in word_freq.items() if c <= 1]
        for w in stale:
            del word_freq[w]
    try:
        generate_report()
    except Exception:
        pass


def record_page(url, text):
    """Commit analytics for a page that passed the similarity check.

    Called by the worker only when is_similar returned False. Updates
    unique_pages, subdomains, longest_page, and word_freq. Also ticks the
    on-disk-report checkpoint.
    """
    page_url = urlunparse(urlparse(url)._replace(fragment=""))
    netloc = urlparse(page_url).netloc.lower()
    with _stats_lock:
        if page_url in unique_pages:
            # Same URL parsed twice (e.g. two paths normalize the same).
            # Already counted; nothing to do.
            return
        unique_pages.add(page_url)
        subdomains[netloc] = subdomains.get(netloc, 0) + 1

    if text:
        words = text.split()
        if len(words) >= NUM_WORDS:
            filtered = []
            for w in words:
                wl = w.lower()
                if wl.isalpha() and wl not in STOP_WORDS:
                    filtered.append(wl)
            global longest_page
            with _stats_lock:
                if len(words) > longest_page[1]:
                    longest_page = (page_url, len(words))
                word_freq.update(filtered)

    _checkpoint_if_due()


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
            f.write(f"   {netloc}, {subdomains[netloc]}\n")
    print(f"[report] Written to {path}")


atexit.register(generate_report)
