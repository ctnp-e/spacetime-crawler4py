'''
urlhash | url | completed
'''
import os
import sqlite3

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        save_path = self.config.save_file

        if restart:
            # WAL mode leaves -wal and -shm sidecars; nuke all three or
            # SQLite will replay the write-ahead log and resurrect data.
            removed = []
            for suffix in ("", "-wal", "-shm"):
                target = save_path + suffix
                if os.path.exists(target):
                    os.remove(target)
                    removed.append(target)
            if removed:
                self.logger.info(f"Restart requested, cleared: {removed}")
        elif not os.path.exists(save_path):
            self.logger.info(
                f"Did not find save file {save_path}, starting from seed.")

        # every method call on self.conn is a transaction, so no need to manually commit
        # this is for atomicity and crash safety
        # if the crawler crashes in the middle of an operation, database wont be left in a half-updated state
        # crash usually means just restart anyways, but people online say this is fire
        self.conn = sqlite3.connect(save_path, isolation_level=None)

        # Write-Ahead Logging (WAL) is a standard technique used to ensure data integrity and atomicity
        # journal mode is so that if it crashes it roll forward or roll back!
        self.conn.execute("PRAGMA journal_mode=WAL") 
        # primary key implies uniqueness --> NO DUPLICATES
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                urlhash   TEXT PRIMARY KEY,
                url       TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0
            )
        """)

        # only finds pending URLS, so index is small and fast
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending "
            "ON urls(completed) WHERE completed = 0")

        if self._is_empty():
            # adds new urls
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            tbd = self.conn.execute(
                "SELECT COUNT(*) FROM urls WHERE completed = 0").fetchone()[0]
            total = self.conn.execute(
                "SELECT COUNT(*) FROM urls").fetchone()[0]
            self.logger.info(
                # in progress snapshot --> loading bar
                f"urls discovered: {total} \t urls pending: {tbd}")

    def _is_empty(self):
        return self.conn.execute("SELECT 1 FROM urls LIMIT 1").fetchone() is None

    def get_tbd_url(self):

        while True:
            # finds a pending url, if any
            row = self.conn.execute(
                "SELECT urlhash, url FROM urls WHERE completed = 0 LIMIT 1"
            ).fetchone()

            if row is None:
                return None
            
            urlhash, url = row
            # If is_valid has tightened since this url was queued, drop it
            # and try the next one.
            if not is_valid(url):
                # if its not valid, mark it complete so we dont keep trying to crawl it
                self.conn.execute(
                    "UPDATE urls SET completed = 1 WHERE urlhash = ?", (urlhash,))
                continue
            return url

    def add_url(self, url):
        
        # tries to add the url with the hash (assuming it's never been seen)
        # if it's already in the sql table once, this is not good - uniqueness is at play
        # so should throw an error, but doesnt - pretend url is not real. 
        # ingored, and toss the url we are looking at
        
        url = normalize(url)
        urlhash = get_urlhash(url)
        self.conn.execute(
            "INSERT OR IGNORE INTO urls (urlhash, url, completed) VALUES (?, ?, 0)",
            (urlhash, url))

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        cur = self.conn.execute(
            # just marking as complete.
            "UPDATE urls SET completed = 1 WHERE urlhash = ?", (urlhash,))
        if cur.rowcount == 0:
            self.logger.error(
                # shouldnt be possible no lie
                f"Completed url {url}, but have not seen it before.")
