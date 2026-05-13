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

                # Single-parse path: text + valid_links from one BeautifulSoup build.
                # Saves the double-parse that bunched GC pressure under 4 workers.
                text, scraped_urls = scraper.process_response(tbd_url, resp)
                duplicate, similarity_type = self.sim.is_similar(tbd_url, text)

                if duplicate:
                    self.logger.info(
                        f"Skipped {tbd_url} due to {similarity_type} similarity.")
                else:
                    # Harvest links regardless of whether text was extractable —
                    # directory listings / faceted pages have valid hrefs even
                    # when our chrome-stripping leaves take_text returning "".
                    if text:
                        self.logger.info(
                            f"Downloaded {tbd_url}, status <{resp.status}>, "
                            f"using cache {self.config.cache_server}.")
                    else:
                        self.logger.info(
                            f"Downloaded {tbd_url} (no extractable text, "
                            f"harvesting links only), status <{resp.status}>.")
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

            