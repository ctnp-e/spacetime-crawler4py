import os
import dbm, dbm.dumb
import shelve
dbm._defaultmod = dbm.dumb

from threading import Condition
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue()
        self._cond = Condition()
        self._in_flight = 0
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
            
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.put(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        # Block until a URL is available or the crawl is truly done.
        # "Truly done" = queue empty AND no worker is mid-crawl (in-flight == 0).
        # Without the in-flight check a worker could drain the queue, return None,
        # and stop — while another worker is still processing a page about to add URLs.
        with self._cond:
            while True:
                try:
                    url = self.to_be_downloaded.get_nowait()
                    self._in_flight += 1
                    return url
                except Empty:
                    if self._in_flight == 0:
                        return None
                    self._cond.wait(timeout=1.0)

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self._cond:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)
                self._cond.notify_all()

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self._cond:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.save[urlhash] = (url, True)
            self.save.sync()
            self._in_flight -= 1
            self._cond.notify_all()
