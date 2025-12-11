"""
Utility: Single Page HTML Scraper

This module provides simple, robust helpers for fetching raw HTML for:
- A single, known URL (one page per request)
- A pair of nested loops (i, j) that produce minor variations in the URL per request

It offers both synchronous (requests-based) and asynchronous (aiohttp-based) APIs, plus helpers to generate looped URLs.

Usage (sync, single URL):
    from app.utils.single_page_html_scraper import fetch_html_sync

    page = fetch_html_sync("https://example.com/page?id=1")
    if page.error is None and page.status == 200:
        print(page.html)

Usage (async, single URL):
    import asyncio
    from app.utils.single_page_html_scraper import fetch_html_async

    async def main():
        page = await fetch_html_async("https://example.com/page?id=1")
        if page.error is None and page.status == 200:
            print(page.html)

    asyncio.run(main())

Usage (looped pattern; build URLs with i and j and fetch one-by-one, sync):
    from app.utils.single_page_html_scraper import iter_loop_urls, fetch_html_sync

    # Example: https://example.com/category/{i}/item/{j}
    for url in iter_loop_urls(
        url_template="https://example.com/category/{i}/item/{j}",
        i_values=range(1, 3),    # i = 1..2
        j_values=range(10, 13),  # j = 10..12
    ):
        page = fetch_html_sync(url)
        print(url, page.status, len(page.html or ""))

Usage (looped pattern with zero padding and query params):
    from app.utils.single_page_html_scraper import iter_loop_urls, fetch_html_sync

    template = "https://example.com/report?month={i}&day={j}&mode=summary"
    for url in iter_loop_urls(
        url_template=template,
        i_values=range(1, 3),    # months 1..2
        j_values=range(1, 4),    # days 1..3
        zero_pad_i=2,            # i -> "01", "02", ...
        zero_pad_j=2,            # j -> "01", "02", ...
    ):
        page = fetch_html_sync(url)
        print(url, page.status)

Notes:
- Choose fetch_html_sync for synchronous code (e.g., scripts).
- Choose fetch_html_async in async contexts (e.g., within FastAPI endpoints or async workers).
- Both functions fetch exactly one page per call.

"""

from __future__ import annotations

import asyncio
import datetime as _dt
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union

import aiohttp
import email.utils as email_utils
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------
# Data structures and constants
# -----------------------------


@dataclass
class HTMLPage:
    """Represents the result of an HTML fetch operation."""

    url: str
    final_url: Optional[str]
    status: Optional[int]
    headers: Dict[str, str]
    html: Optional[str]
    fetched_at: str
    elapsed_seconds: float
    error: Optional[str] = None


DEFAULT_HEADERS: Dict[str, str] = {
    # A commonly accepted desktop UA string helps avoid basic bot blocks
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Connection": "close",
}


class HtmlFetchError(Exception):
    """Raised when fetching an HTML page fails irrecoverably."""


# -----------------------------
# URL generation for i/j loops
# -----------------------------


def iter_loop_urls(
    url_template: str,
    i_values: Iterable[Union[int, str]],
    j_values: Iterable[Union[int, str]],
    *,
    zero_pad_i: Optional[int] = None,
    zero_pad_j: Optional[int] = None,
    i_transform=None,
    j_transform=None,
    extra_format: Optional[Dict[str, str]] = None,
) -> Iterator[str]:
    """
    Generate URLs by formatting `url_template` with i and j (also supports
    {rating}/{page} placeholders for clarity).

    The template should include placeholders {i}/{j} or {rating}/{page}. Examples:
        "https://example.com/category/{i}/item/{j}"
        or "https://example.com/report?month={i}&day={j}"

    Args:
        url_template: A Python format string containing {i}/{j} or {rating}/{page}
        i_values: Iterable of i values (e.g., range(1, 3))
        j_values: Iterable of j values (e.g., range(10, 13))
        zero_pad_i: If provided, zero-pad i to this width (e.g., 2 -> 01, 02)
        zero_pad_j: If provided, zero-pad j to this width
        i_transform: Optional callable to transform i before formatting
        j_transform: Optional callable to transform j before formatting
        extra_format: Optional dict of additional placeholders for format()

    Yields:
        Formatted URL strings.
    """
    extra_format = extra_format or {}

    for i in i_values:
        if zero_pad_i and isinstance(i, int):
            i_val = str(i).zfill(zero_pad_i)
        else:
            i_val = str(i)
        if i_transform:
            i_val = str(i_transform(i_val))

        for j in j_values:
            if zero_pad_j and isinstance(j, int):
                j_val = str(j).zfill(zero_pad_j)
            else:
                j_val = str(j)
            if j_transform:
                j_val = str(j_transform(j_val))

                # Support both {i}/{j} and {rating}/{page} placeholders by
                # supplying all relevant keys when calling format(). We avoid
                # depending on which placeholders are present -- Python's
                # format() will only substitute the keys used.
            yield url_template.format(
                i=i_val,
                j=j_val,
                rating=i_val,
                page=j_val,
                **extra_format,
            )


# -----------------------------
# Asynchronous HTML fetch (aiohttp)
# -----------------------------


async def fetch_html_async(
    url: str,
    *,
    timeout: float = 30.0,
    max_retries: int = 4,
    backoff_factor: float = 0.5,
    headers: Optional[Dict[str, str]] = None,
    follow_redirects: bool = True,
    verify_ssl: bool = True,
    raise_for_status: bool = False,
    encoding: Optional[str] = None,
) -> HTMLPage:
    """
    Fetch raw HTML from a single URL using aiohttp with retry and timeout.

    Args:
        url: The URL to fetch
        timeout: Per-attempt timeout in seconds
        max_retries: Number of retries (in addition to the first attempt)
        backoff_factor: Time to wait between retries (exponential backoff: factor * 2^attempt)
        headers: Optional request headers (merged over DEFAULT_HEADERS)
        follow_redirects: Whether to follow redirects
        verify_ssl: Verify SSL certificates
        raise_for_status: If True, raise on non-2xx status
        encoding: Force response encoding; if None, rely on server/auto-detection

    Returns:
        HTMLPage object with content and metadata.
    """
    session_headers = {**DEFAULT_HEADERS, **(headers or {})}
    start_ts = time.perf_counter()

    last_err: Optional[str] = None
    attempt = 0

    # aiohttp ClientTimeout controls total operation per request attempt
    client_timeout = aiohttp.ClientTimeout(total=timeout)

    while attempt <= max_retries:
        try:
            connector = aiohttp.TCPConnector(ssl=verify_ssl)
            async with aiohttp.ClientSession(
                timeout=client_timeout, headers=session_headers, connector=connector
            ) as session:
                async with session.get(url, allow_redirects=follow_redirects) as resp:
                    status = resp.status
                    final_url = str(resp.url)
                    # Ensure we get text with correct encoding if provided
                    if encoding:
                        raw = await resp.read()
                        html = raw.decode(encoding, errors="replace")
                    else:
                        html = await resp.text()

                    elapsed = time.perf_counter() - start_ts

                    # If the server rate-limits us (429), honor Retry-After header and retry
                    if status == 429:
                        # Respect Retry-After if present, otherwise use exponential backoff.
                        if attempt >= max_retries:
                            # No retries left; we'll fall through and return the 429 page
                            pass
                        else:
                            retry_after = resp.headers.get("Retry-After")
                            delay = None
                            if retry_after:
                                # Retry-After can be a number of seconds or an HTTP-date
                                try:
                                    delay = float(retry_after)
                                except Exception:
                                    try:
                                        retry_dt = email_utils.parsedate_to_datetime(
                                            retry_after
                                        )
                                        delay = max(
                                            0.0,
                                            (
                                                retry_dt - _dt.datetime.utcnow()
                                            ).total_seconds(),
                                        )
                                    except Exception:
                                        delay = None
                            if delay is None:
                                # Use exponential backoff starting at 8 seconds
                                base_delay = 8.0
                                delay = base_delay * (2**attempt)
                            # Add small random jitter to avoid thundering herd
                            jitter = random.uniform(0, min(1.0, delay * 0.1))
                            chosen_delay = delay + jitter
                            print(
                                f"[WARN] HTTP 429 for {url} - retrying after {chosen_delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                            )
                            await asyncio.sleep(chosen_delay)
                            # increment attempt counter and retry
                            attempt += 1
                            continue
                        retry_after = resp.headers.get("Retry-After")
                        delay = None
                        if retry_after:
                            # Retry-After can be a number of seconds or an HTTP-date
                            try:
                                delay = float(retry_after)
                            except Exception:
                                try:
                                    retry_dt = email_utils.parsedate_to_datetime(
                                        retry_after
                                    )
                                    delay = max(
                                        0.0,
                                        (
                                            retry_dt - _dt.datetime.utcnow()
                                        ).total_seconds(),
                                    )
                                except Exception:
                                    delay = None
                        if delay is None:
                            delay = backoff_factor * (2**attempt)
                        await asyncio.sleep(delay)
                        # Try again (increment attempt and loop)
                        attempt += 1
                        continue

                    # Optional raise on non-2xx after we obtain the body
                    if raise_for_status and not (200 <= status < 300):
                        raise HtmlFetchError(f"HTTP {status} for {url}")

                    # Convert MultiDict headers -> plain dict[str, str]
                    headers_out = {k: v for k, v in resp.headers.items()}

                    return HTMLPage(
                        url=url,
                        final_url=final_url,
                        status=status,
                        headers=headers_out,
                        html=html,
                        fetched_at=_dt.datetime.utcnow().isoformat() + "Z",
                        elapsed_seconds=elapsed,
                        error=None,
                    )

        except asyncio.TimeoutError:
            last_err = f"Timeout after {timeout}s"
        except Exception as e:
            last_err = str(e)

        attempt += 1
        if attempt <= max_retries:
            await asyncio.sleep(backoff_factor * (2 ** (attempt - 1)))

    # All attempts failed
    elapsed = time.perf_counter() - start_ts
    return HTMLPage(
        url=url,
        final_url=None,
        status=None,
        headers={},
        html=None,
        fetched_at=_dt.datetime.utcnow().isoformat() + "Z",
        elapsed_seconds=elapsed,
        error=last_err or "Unknown error",
    )


# -----------------------------
# Synchronous HTML fetch (requests)
# -----------------------------


def _requests_session_with_retry(
    total_retries: int,
    backoff_factor: float,
    status_forcelist: Optional[List[int]] = None,
) -> requests.Session:
    """Create a requests Session configured with retry adapter."""
    status_forcelist = status_forcelist or [429, 500, 502, 503, 504]

    retry_cfg = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        raise_on_status=False,
        raise_on_redirect=False,
    )

    adapter = HTTPAdapter(max_retries=retry_cfg)
    sess = requests.Session()
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def fetch_html_sync(
    url: str,
    *,
    timeout: float = 30.0,
    max_retries: int = 4,
    backoff_factor: float = 0.5,
    headers: Optional[Dict[str, str]] = None,
    follow_redirects: bool = True,
    verify_ssl: bool = True,
    raise_for_status: bool = False,
    encoding: Optional[str] = None,
) -> HTMLPage:
    """
    Fetch raw HTML from a single URL synchronously using requests with retry and timeout.

    Args:
        url: The URL to fetch
        timeout: Per-attempt timeout in seconds
        max_retries: Number of retries (in addition to the first attempt)
        backoff_factor: Time to wait between retries (exponential backoff)
        headers: Optional request headers (merged over DEFAULT_HEADERS)
        follow_redirects: Whether to follow redirects
        verify_ssl: Verify SSL certificates
        raise_for_status: If True, raise on non-2xx status
        encoding: Force response encoding; if None, rely on server/auto-detection

    Returns:
        HTMLPage object with content and metadata.
    """
    session_headers = {**DEFAULT_HEADERS, **(headers or {})}
    start_ts = time.perf_counter()

    with _requests_session_with_retry(
        total_retries=max_retries, backoff_factor=backoff_factor
    ) as sess:
        try:
            resp = sess.get(
                url,
                headers=session_headers,
                timeout=timeout,
                allow_redirects=follow_redirects,
                verify=verify_ssl,
            )
            if encoding:
                resp.encoding = encoding

            if raise_for_status:
                # Will raise HTTPError for 4xx/5xx
                resp.raise_for_status()

            elapsed = time.perf_counter() - start_ts

            return HTMLPage(
                url=url,
                final_url=str(resp.url),
                status=resp.status_code,
                headers=dict(resp.headers),
                html=resp.text,
                fetched_at=_dt.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=elapsed,
                error=None,
            )
        except Exception as e:
            elapsed = time.perf_counter() - start_ts
            return HTMLPage(
                url=url,
                final_url=None,
                status=None,
                headers={},
                html=None,
                fetched_at=_dt.datetime.utcnow().isoformat() + "Z",
                elapsed_seconds=elapsed,
                error=str(e),
            )


# -----------------------------
# Convenience helpers
# -----------------------------


async def fetch_looped_pages_async(
    url_template: str,
    i_values: Iterable[Union[int, str]],
    j_values: Iterable[Union[int, str]],
    *,
    zero_pad_i: Optional[int] = None,
    zero_pad_j: Optional[int] = None,
    i_transform=None,
    j_transform=None,
    per_request_timeout: float = 30.0,
    max_retries: int = 2,
    backoff_factor: float = 0.5,
    headers: Optional[Dict[str, str]] = None,
    follow_redirects: bool = True,
    verify_ssl: bool = True,
    raise_for_status: bool = False,
    encoding: Optional[str] = None,
) -> Iterator[Tuple[str, HTMLPage]]:
    """
    Iterate over (url, HTMLPage) by fetching one page per URL generated by i/j loops, asynchronously.
    This function yields results as they are fetched sequentially.

    Note: This is intentionally sequential (single page per request) to match the requirement.
    """
    for url in iter_loop_urls(
        url_template=url_template,
        i_values=i_values,
        j_values=j_values,
        zero_pad_i=zero_pad_i,
        zero_pad_j=zero_pad_j,
        i_transform=i_transform,
        j_transform=j_transform,
    ):
        page = await fetch_html_async(
            url=url,
            timeout=per_request_timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            headers=headers,
            follow_redirects=follow_redirects,
            verify_ssl=verify_ssl,
            raise_for_status=raise_for_status,
            encoding=encoding,
        )
        yield url, page


def fetch_looped_pages_sync(
    url_template: str,
    i_values: Iterable[Union[int, str]],
    j_values: Iterable[Union[int, str]],
    *,
    zero_pad_i: Optional[int] = None,
    zero_pad_j: Optional[int] = None,
    i_transform=None,
    j_transform=None,
    per_request_timeout: float = 30.0,
    max_retries: int = 2,
    backoff_factor: float = 0.5,
    headers: Optional[Dict[str, str]] = None,
    follow_redirects: bool = True,
    verify_ssl: bool = True,
    raise_for_status: bool = False,
    encoding: Optional[str] = None,
) -> Iterator[Tuple[str, HTMLPage]]:
    """
    Iterate over (url, HTMLPage) by fetching one page per URL generated by i/j loops, synchronously.
    This function yields results as they are fetched sequentially.

    Note: This is intentionally sequential (single page per request) to match the requirement.
    """
    for url in iter_loop_urls(
        url_template=url_template,
        i_values=i_values,
        j_values=j_values,
        zero_pad_i=zero_pad_i,
        zero_pad_j=zero_pad_j,
        i_transform=i_transform,
        j_transform=j_transform,
    ):
        page = fetch_html_sync(
            url=url,
            timeout=per_request_timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            headers=headers,
            follow_redirects=follow_redirects,
            verify_ssl=verify_ssl,
            raise_for_status=raise_for_status,
            encoding=encoding,
        )
        yield url, page


__all__ = [
    "HTMLPage",
    "HtmlFetchError",
    "DEFAULT_HEADERS",
    "fetch_html_async",
    "fetch_html_sync",
    "iter_loop_urls",
    "fetch_looped_pages_async",
    "fetch_looped_pages_sync",
]
