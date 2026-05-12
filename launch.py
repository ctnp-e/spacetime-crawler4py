from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from similarity import Similarity


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    # One Similarity instance shared across all worker threads — see
    # crawler/__init__.py for how it's threaded through to each worker.
    sim = Similarity()
    crawler = Crawler(config, restart, sim=sim)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
