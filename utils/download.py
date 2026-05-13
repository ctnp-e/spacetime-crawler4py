import requests
import cbor
import time
import threading
from urllib.parse import urlparse

from utils.response import Response

# Per-host pacing for multithreaded crawls
# Maps netloc -> [Lock, last_request_monotonic].
# Outer lock guards the dict
# each host Lock serializes requests to that host
# so workers chasing the same domain in parallel still honor POLITENESS
_host_state_lock = threading.Lock()
_host_state = {}


def _get_host_record(netloc):
    with _host_state_lock:
        rec = _host_state.get(netloc)
        if rec is None:
            rec = [threading.Lock(), 0.0]
            _host_state[netloc] = rec
        return rec


def download(url, config, logger=None):
    # Politeness is per target host, so we use the netloc as the key for locking and delay tracking
    netloc = urlparse(url).netloc.lower()
    rec = _get_host_record(netloc)
    host_lock = rec[0]
    delay = float(getattr(config, "time_delay", 0.5))

    host, port = config.cache_server

    with host_lock:
        wait = (rec[1] + delay) - time.monotonic()
        if wait > 0:
            time.sleep(wait)
            # attempt at fixing the . uh. sigkill i get
        try:
            resp = requests.get(
                f"http://{host}:{port}/",
                params=[("q", f"{url}"), ("u", f"{config.user_agent}")],
                timeout=(10, 30)) # if it goes on for too long
        except requests.exceptions.Timeout:
            if logger:
                logger.error(f"Timeout fetching {url}")
            rec[1] = time.monotonic()
            return Response({"error": f"Timeout fetching {url}", "status": 408, "url": url})
        rec[1] = time.monotonic()

    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    if logger:
        logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
