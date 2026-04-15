# -*- coding: utf-8 -*-
"""
Shared HTTP utilities for Seam Analytics.

Centralises HTTP session creation and timeout constants so every module
uses the same retry / timeout policy without duplicating the boilerplate.
"""

import requests
import requests.adapters
from urllib3.util.retry import Retry

# ── Timeout constants (seconds) ─────────────────────────────────────
TIMEOUT_DEFAULT  = 10   # schedule, live feed, boxscore
TIMEOUT_SHORT    = 5    # single-player endpoints (people, handedness)
TIMEOUT_LONG     = 30   # bulk / slow endpoints (statcast, backfill)
TIMEOUT_DOWNLOAD = 60   # large file downloads (streaming)


def create_http_session(total_retries=3, backoff_factor=0.5,
                        status_forcelist=None):
    """Create a ``requests.Session`` with automatic retry on transient errors.

    Parameters
    ----------
    total_retries : int
        Maximum number of retries per request.
    backoff_factor : float
        Exponential back-off factor between retries.
    status_forcelist : list[int] | None
        HTTP status codes that trigger a retry.  Defaults to
        ``[429, 502, 503, 504]``.

    Returns
    -------
    requests.Session
    """
    if status_forcelist is None:
        status_forcelist = [429, 502, 503, 504]
    retry = Retry(total=total_retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
