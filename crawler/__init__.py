from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from similarity import Similarity

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker, sim=None):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        # One Similarity instance shared across all workers so near-dup
        # detection sees the union of pages everyone has crawled.
        self.sim = sim if sim is not None else Similarity()

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.sim)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
