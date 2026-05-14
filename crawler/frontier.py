import gc
import os
import dbm, dbm.dumb
import shelve
# Setting _defaultmod alone bypasses dbm's normal init loop, which leaves
# _modules empty — so reopening an EXISTING dbm.dumb file fails with
# "db type is dbm.dumb, but the module is not available". Populate both.
dbm._defaultmod = dbm.dumb
dbm._modules["dbm.dumb"] = dbm.dumb

from threading import Condition
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

_SYNC_EVERY = 1000     # ops between shelve.sync() flushes — see _maybe_sync
_MEM_QUEUE_CAP = 10000 # in-memory Queue ceiling. Overflow spills to disk.
_REFILL_BELOW = 1000   # refill from disk when in-memory queue drops below this
_REFILL_BATCH = 5000   # max URLs pulled from disk per refill


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue()
        self._cond = Condition()
        self._in_flight = 0
        # dbm.dumb rewrites its index on every sync(). At ~100 links/page that
        # was millions of disk flushes per crawl. Batch instead — at most
        # _SYNC_EVERY ops of progress is at risk on SIGKILL, and similarity
        # catches accidental re-crawls.
        self._sync_counter = 0

        # Two-tier frontier: hot URLs in-memory (capped at _MEM_QUEUE_CAP),
        # overflow appended to a plain text file (one URL per line). The
        # offset file records where to resume reading after a refill so
        # URLs aren't replayed across restarts.
        self._overflow_path = self.config.save_file + ".overflow"
        self._overflow_offset_path = self._overflow_path + ".offset"
        self._overflow_offset = 0

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        # On --restart, wipe the disk overflow too.
        if restart:
            for p in (self._overflow_path, self._overflow_offset_path):
                if os.path.exists(p):
                    os.remove(p)
        elif os.path.exists(self._overflow_offset_path):
            try:
                with open(self._overflow_offset_path) as f:
                    self._overflow_offset = int(f.read().strip() or "0")
            except (OSError, ValueError):
                self._overflow_offset = 0

        # Append handle stays open for cheap writes.
        self._overflow_writer = open(self._overflow_path, "a", encoding="utf-8")

        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
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
                if self.to_be_downloaded.qsize() < _MEM_QUEUE_CAP:
                    self.to_be_downloaded.put(url)
                else:
                    self._overflow_append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def _overflow_append(self, url):
        # Caller must hold self._cond. Write+flush so a concurrent refill
        # reading from the same file sees fully-written lines.
        self._overflow_writer.write(url + "\n")
        self._overflow_writer.flush()

    def _overflow_refill(self):
        # Caller must hold self._cond. Pull up to _REFILL_BATCH URLs from
        # disk into the memory queue and advance the offset file.

        if not os.path.exists(self._overflow_path):
            return
        if os.path.getsize(self._overflow_path) <= self._overflow_offset:
            return
        
        # Make sure pending appends are visible to the reader.
        
        self._overflow_writer.flush()
        added = 0
        with open(self._overflow_path, "r", encoding="utf-8") as f:
            f.seek(self._overflow_offset)
            for _ in range(_REFILL_BATCH):
                line = f.readline()
                if not line:
                    break
                url = line.rstrip("\n")
                if url:
                    self.to_be_downloaded.put(url)
                    added += 1
            self._overflow_offset = f.tell()
        tmp = self._overflow_offset_path + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(self._overflow_offset))
        os.replace(tmp, self._overflow_offset_path)
        if added:
            self._cond.notify_all()

    def get_tbd_url(self):
        # Block until a URL is available or the crawl is truly done.
        # "Truly done" = queue empty AND no worker is mid-crawl (in-flight == 0).
        with self._cond:
            while True:
                if self.to_be_downloaded.qsize() < _REFILL_BELOW:
                    self._overflow_refill()
                try:
                    url = self.to_be_downloaded.get_nowait()
                    self._in_flight += 1
                    return url
                except Empty:
                    if self._in_flight == 0:
                        return None
                    self._cond.wait(timeout=1.0)

    def _maybe_sync(self):
        # Caller must hold self._cond. Batches shelve.sync() to one flush
        # per _SYNC_EVERY ops instead of one per write.
        self._sync_counter += 1
        if self._sync_counter >= _SYNC_EVERY:
            self._sync_counter = 0
            self.save.sync()
            gc.collect()

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self._cond:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self._maybe_sync()
                if self.to_be_downloaded.qsize() < _MEM_QUEUE_CAP:
                    self.to_be_downloaded.put(url)
                else:
                    self._overflow_append(url)
                self._cond.notify_all()

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self._cond:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.save[urlhash] = (url, True)
            self._maybe_sync()
            self._in_flight -= 1
            self._cond.notify_all()
