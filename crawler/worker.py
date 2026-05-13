from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, sim):
        self.logger = get_logger(f"worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

        # Shared across all workers — see launch.py / Crawler.__init__.
        # Lets near-duplicate detection see pages other workers have seen.
        self.sim = sim

    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # try/finally is the deadlock guard
            try:
                # download() now self-paces per-host, so no extra sleep needed here.
                resp = download(tbd_url, self.config, self.logger)

                # take text, and then compare
                # YOU WANT TO TAKE THE TEXT EVEN IF NEAR DUPLICATE.
                text = scraper.take_text(tbd_url, resp)
                duplicate, similarity_type = self.sim.is_similar(tbd_url, text)

                if duplicate:
                    # Near/exact dup — skip both link harvest AND content tracking.
                    # Re-walking dups would just feed the same hrefs back to the
                    # frontier (already-seen URL dedup catches most, but it's
                    # wasted work and a near-dup's links rarely point anywhere new).
                    self.logger.info(
                        f"Skipped {tbd_url} due to {similarity_type} similarity.")
                else:
                    # Harvest links regardless of whether take_text yielded meaningful content
                    if text:
                        self.logger.info(
                            f"Downloaded {tbd_url}, status <{resp.status}>, "
                            f"using cache {self.config.cache_server}.")
                    else:
                        self.logger.info(
                            f"Downloaded {tbd_url} (no extractable text, "
                            f"harvesting links only), status <{resp.status}>.")
                    scraped_urls = scraper.scraper(tbd_url, resp)
                    for scraped_url in scraped_urls:
                        self.frontier.add_url(scraped_url)

            except Exception as e:
                # log with traceback but make sure it can keep running
                self.logger.error(
                    f"Unhandled exception on {tbd_url}: {e!r}",
                    exc_info=True,
                )

            finally:
                # crawl can always terminate cleanly when the frontier drains.
                self.frontier.mark_url_complete(tbd_url)

            