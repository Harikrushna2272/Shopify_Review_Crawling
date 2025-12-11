#!/usr/bin/env python3
"""
Pipeline: Crawl pages, extract reviews, and write a single aggregated CSV.

This script:
1) Generates URLs via simple two-loop (i, j) pattern or accepts explicit URLs.
2) Fetches raw HTML (one page per request) using the HTML scraper utilities.
3) Extracts review records (name, review, date, location, usage_duration) using the regex extractor.
4) Writes a single aggregated CSV across all pages.

Usage examples (run from repo root):

# Example 1: Two nested loops with URL template
python crawl-service/app/pipeline/temp.py \
    --mode loops \
    --template "https://example.com/category/{rating}/item/{page}" \
    --ratings-start 1 --ratings-end 3 \
    --page-start 10 --page-end 13 \
    --output outputs/reviews_aggregated.csv

# Example 2: Provide explicit URLs as arguments
python crawl-service/app/pipeline/temp.py \
    --mode urls \
    --urls "https://example.com/page1" "https://example.com/page2" \
    --output outputs/reviews_aggregated.csv

Notes:
- This pipeline fetches exactly one page per request.
- It expects the Shopify-like structure for reviews as handled by the regex extractor.
- Adjust timeouts and retries via CLI flags if needed.

"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, List, Tuple, Optional
import os
import resource
import platform

try:
    import psutil
except Exception:
    psutil = None
import urllib.parse
import itertools
import asyncio
import datetime as _dt

# Import utilities
# - HTML fetching (sync), and loop URL generator
from html_scraper import (
    fetch_html_sync,
    fetch_html_async,
    iter_loop_urls,
    HTMLPage,
)

# - Review extraction
from extract_reviews_to_csv import (
    extract_reviews,
    ReviewRecord,
)
from pathlib import Path
import re


def generate_urls_from_loops(
    template: str,
    ratings_start: int,
    ratings_end: int,
    page_start: int,
    page_end: Optional[int],
    zero_pad_i: int | None = None,
    zero_pad_j: int | None = None,
) -> Iterable[str]:
    """
    Generate URLs using two nested loops with an {i}/{j} or {rating}/{page} URL template.

    Args:
        template: The URL format string with placeholders {i} and {j}
        ratings_start: Inclusive start for ratings
        ratings_end: Exclusive end for ratings
        page_start: Inclusive start for page
            page_end: Exclusive end for page or None to iterate pages until a zero-review page.
        zero_pad_i: Optional zero padding width for i values
        zero_pad_j: Optional zero padding width for j values

    Returns:
        Iterable of fully formatted URLs.
    """
    # iter_loop_urls expects i_values/j_values; keep those names internally
    if page_end is None:
        raise ValueError("generate_urls_from_loops requires page_end to be specified")

    return iter_loop_urls(
        url_template=template,
        i_values=range(ratings_start, ratings_end),
        j_values=range(page_start, page_end),
        zero_pad_i=zero_pad_i,
        zero_pad_j=zero_pad_j,
    )


def fetch_and_extract(
    url: str, timeout: float, max_retries: int
) -> Tuple[str, List[ReviewRecord]]:
    """
    Fetch a single URL and extract reviews from its HTML.

    Returns:
        (url, list_of_reviews)
    """
    page: HTMLPage = fetch_html_sync(
        url=url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=0.5,
        headers=None,
        follow_redirects=True,
        verify_ssl=True,
        raise_for_status=False,
        encoding=None,
    )
    if page.error:
        print(f"[WARN] Failed to fetch {url}: {page.error}")
        return url, []

    html_text = page.html or ""
    if not html_text.strip():
        print(f"[WARN] Empty HTML from {url}")
        return url, []

    reviews = extract_reviews(html_text)
    print(f"[INFO] {url}: extracted {len(reviews)} reviews")
    # Debugging: when zero reviews are extracted but HTML exists, save snippet for inspection
    if len(reviews) == 0 and html_text.strip():
        simple_count = len(
            re.findall(r"tw-break-words", html_text, flags=re.IGNORECASE)
        )
        print(
            f"[DEBUG] {url}: status={page.status}, html_len={len(html_text)}, 'tw-break-words' count={simple_count}"
        )
        debug_dir = Path("outputs") / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        page_val = qs.get("page", [None])[0] or qs.get("j", [None])[0] or "unknown"
        rating_val = (
            qs.get("ratings[]", [None])[0]
            or qs.get("ratings%5B%5D", [None])[0]
            or qs.get("rating", [None])[0]
            or ""
        )
        safe_name = f"rating{rating_val}_page{page_val}.html"
        debug_path = debug_dir / safe_name
        # Limit to first 30000 chars to avoid massive files
        debug_path.write_text(html_text[:30000], encoding="utf-8", errors="ignore")
        print(f"[DEBUG] Saved HTML snippet to {debug_path}")
    return url, reviews


async def fetch_and_extract_async(
    url: str, timeout: float, max_retries: int
) -> Tuple[str, List[ReviewRecord]]:
    """
    Async wrapper for fetching a URL and extracting reviews.

    This uses the aiohttp-based `fetch_html_async` and runs the
    synchronous `extract_reviews` in a thread to avoid blocking the loop.
    """
    page: HTMLPage = await fetch_html_async(
        url=url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=1.5,
        headers=None,
        follow_redirects=True,
        verify_ssl=True,
        raise_for_status=False,
        encoding=None,
    )
    if page.error:
        print(f"[WARN] Failed to fetch {url}: {page.error}")
        return url, []

    html_text = page.html or ""
    if not html_text.strip():
        print(f"[WARN] Empty HTML from {url}")
        return url, []

    # Run the potentially CPU-bound/sync extractor in a thread
    reviews = await asyncio.to_thread(extract_reviews, html_text)
    print(f"[INFO] {url}: extracted {len(reviews)} reviews (async)")
    if len(reviews) == 0 and html_text.strip():
        simple_count = len(
            re.findall(r"tw-break-words", html_text, flags=re.IGNORECASE)
        )
        # print(
        #     f"[DEBUG] {url}: status={page.status}, html_len={len(html_text)}, 'tw-break-words' count={simple_count}"
        # )
        # debug_dir = Path("outputs") / "debug"
        # debug_dir.mkdir(parents=True, exist_ok=True)
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        page_val = qs.get("page", [None])[0] or qs.get("j", [None])[0] or "unknown"
        rating_val = (
            qs.get("ratings[]", [None])[0]
            or qs.get("ratings%5B%5D", [None])[0]
            or qs.get("rating", [None])[0]
            or ""
        )
        # safe_name = f"rating{rating_val}_page{page_val}.html"
        # debug_path = debug_dir / safe_name
        # debug_path.write_text(html_text[:30000], encoding="utf-8", errors="ignore")
        # print(f"[DEBUG] Saved HTML snippet to {debug_path}")
    return url, reviews


async def monitor_stats(interval: int):
    """Periodically log CPU and memory statistics to console."""
    proc = None
    if psutil:
        try:
            proc = psutil.Process()
            # warm up CPU percent calculation
            proc.cpu_percent(None)
        except Exception:
            proc = None
    try:
        while True:
            now = _dt.datetime.utcnow().isoformat() + "Z"
            if proc:
                try:
                    cpu_pct = proc.cpu_percent(None)
                    mem_rss_mb = proc.memory_info().rss / (1024 * 1024)
                    mem_pct = proc.memory_percent()
                    print(
                        f"[MON] {now} CPU={cpu_pct:.1f}% MEM={mem_rss_mb:.1f}MB ({mem_pct:.1f}%)"
                    )
                except Exception as e:
                    print(f"[MON] {now} error gathering psutil stats: {e}")
            else:
                try:
                    load1, load5, load15 = os.getloadavg()
                    ru = resource.getrusage(resource.RUSAGE_SELF)
                    mem_kb = ru.ru_maxrss
                    mem_mb = mem_kb / 1024.0
                    print(f"[MON] {now} load1={load1:.2f} mem_rss~{mem_mb:.1f}MB")
                except Exception as e:
                    print(f"[MON] {now} error gathering basic stats: {e}")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        # graceful exit
        return


def write_aggregated_csv(records: List[ReviewRecord], out_path: Path) -> None:
    """
    Write all extracted reviews to a single CSV file.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["rating", "name", "review", "date", "location", "usage_duration"]
        )
        for rec in records:
            writer.writerow(
                [
                    getattr(rec, "rating", ""),
                    rec.name,
                    rec.review,
                    rec.date,
                    rec.location,
                    rec.usage_duration,
                ]
            )
    print(f"[DONE] Wrote {len(records)} records to {out_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline: Crawl pages, extract reviews, and write a single aggregated CSV."
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["loops", "urls"],
        required=True,
        help=(
            "Select 'loops' to generate URLs from a looping template (placeholders {rating}/{page} or {i}/{j}) "
            "or 'urls' to provide explicit URLs."
        ),
    )

    # Loop mode parameters
    parser.add_argument(
        "--template",
        type=str,
        help="URL template with placeholders {rating}/{page} or {i}/{j} (loops mode).",
    )
    parser.add_argument(
        "--company",
        type=str,
        default="name_of_company",
        help="Shopify company name to use with the default template when not provided.",
    )
    # Ratings / Page CLI args (preferred names)
    parser.add_argument(
        "--ratings-start",
        dest="ratings_start",
        type=int,
        default=1,
        help="Start of ratings (inclusive).",
    )
    parser.add_argument(
        "--ratings-end",
        dest="ratings_end",
        type=int,
        default=6,
        help="End of ratings (exclusive). By default uses 1..5 (6 exclusive).",
    )
    parser.add_argument(
        "--page-start",
        dest="page_start",
        type=int,
        default=1,
        help="Start of page (inclusive).",
    )
    parser.add_argument(
        "--page-end",
        dest="page_end",
        type=int,
        default=None,
        help="End of page (exclusive). If omitted, pages will iterate until a page returns zero reviews.",
    )
    parser.add_argument(
        "--ignore-empty-pages",
        dest="ignore_empty_pages",
        action="store_true",
        default=False,
        help=(
            "When set, the scraper will not stop when a page returns zero reviews; "
            "continue iterating pages (use with caution)."
        ),
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=None,
        help=(
            "Optional safety upper bound for pages per rating. If omitted and --page-end is not provided, "
            "the scraper may iterate pages indefinitely when --ignore-empty-pages is set."
        ),
    )
    parser.add_argument(
        "--zero-page-threshold",
        dest="zero_page_threshold",
        type=int,
        default=5,
        help=(
            "Number of consecutive pages with zero reviews required to skip to next rating. "
            "Default is 5 (i.e. skip rating after 5 consecutive zero-review pages)."
        ),
    )

    # Backwards-compatibility: allow --i-start/--i-end/--j-start/--j-end
    parser.add_argument("--i-start", dest="i_start", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--i-end", dest="i_end", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--j-start", dest="j_start", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--j-end", dest="j_end", type=int, help=argparse.SUPPRESS)
    parser.add_argument(
        "--zero-pad-i", type=int, default=None, help="Zero pad width for i (optional)."
    )
    parser.add_argument(
        "--zero-pad-j", type=int, default=None, help="Zero pad width for j (optional)."
    )

    # URL mode parameters
    parser.add_argument(
        "--urls", nargs="*", default=[], help="Explicit URLs to crawl (urls mode)."
    )

    # Common
    parser.add_argument(
        "--timeout", type=float, default=30.0, help="Per-request timeout in seconds."
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Max retries per request (default 4).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path("outputs") / "reviews_aggregated.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--monitor",
        action="store_true",
        default=False,
        help="Enable periodic CPU/memory monitoring and log to console.",
    )
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=10,
        help="Monitoring interval in seconds (default: 10).",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    # Run the asynchronous pipeline; wrapper sync entrypoint
    asyncio.run(main_async(args))


async def main_async(args):
    all_records: List[ReviewRecord] = []
    monitor_task = None

    # Normalize numeric args: prefer ratings/page flags; fall back to old i/j names if set
    # If the user provided --i-start etc., map those to ratings/page to minimize breakage.
    if (
        getattr(args, "i_start", None) is not None
        and getattr(args, "ratings_start", None) is None
    ):
        args.ratings_start = args.i_start
    if (
        getattr(args, "i_end", None) is not None
        and getattr(args, "ratings_end", None) is None
    ):
        args.ratings_end = args.i_end
    if (
        getattr(args, "j_start", None) is not None
        and getattr(args, "page_start", None) is None
    ):
        args.page_start = args.j_start
    if (
        getattr(args, "j_end", None) is not None
        and getattr(args, "page_end", None) is None
    ):
        args.page_end = args.j_end

    # Start monitor if enabled so it runs during scraping
    if args.monitor:
        monitor_task = asyncio.create_task(monitor_stats(args.monitor_interval))

    if args.mode == "loops":
        # If no template provided, use the Shopify reviews template with the given company name
        # Template: https://apps.shopify.com/{name_of_company}/reviews?ratings%5B%5D={i}&page={j}
        template = args.template or (
            f"https://apps.shopify.com/{args.company}/reviews?ratings%5B%5D={{rating}}&page={{page}}"
        )

        # Accept templates using either {i}/{j} or {rating}/{page} placeholders.
        if not (
            ("{i}" in template and "{j}" in template)
            or ("{rating}" in template and "{page}" in template)
        ):
            raise ValueError(
                "Template must include {i}/{j} or {rating}/{page} placeholders."
            )

        # Iterate ratings (outer loop), then pages (inner loop). If a page returns
        # zero reviews, stop the inner pages loop (continue to next rating).
        zero_pad_i = args.zero_pad_i
        zero_pad_j = args.zero_pad_j
        for rating in range(args.ratings_start, args.ratings_end):
            # Format rating string with optional zero padding
            if zero_pad_i and isinstance(rating, int):
                rating_val = str(rating).zfill(zero_pad_i)
            else:
                rating_val = str(rating)

            stopped_for_rating = False
            consecutive_zero_pages = 0
            # Determine page iterable. If page_end is None, iterate until no reviews.
            if args.page_end is None:
                page_iter = itertools.count(args.page_start)
            else:
                page_iter = range(args.page_start, args.page_end)
            pages_checked = 0
            for page in page_iter:
                pages_checked += 1
                # After every 22 pages, sleep 7 seconds to reduce rate-limit pressure
                if pages_checked % 22 == 0:
                    print(
                        f"[INFO] Fetched {pages_checked} pages for rating={rating}; sleeping for 8s to avoid rate limits."
                    )
                    await asyncio.sleep(8)
                # After every 22 pages, sleep 7 seconds to reduce rate-limit pressure
                if pages_checked % 22 == 0:
                    print(
                        f"[INFO] Fetched {pages_checked} pages for rating={rating}; sleeping for 8s to avoid rate limits."
                    )
                    await asyncio.sleep(8)
                if args.max_pages is not None and pages_checked > args.max_pages:
                    print(
                        f"[INFO] Reached --max-pages={args.max_pages} for rating={rating}; stopping page loop."
                    )
                    stopped_for_rating = True
                    break
                if zero_pad_j and isinstance(page, int):
                    page_val = str(page).zfill(zero_pad_j)
                else:
                    page_val = str(page)

                try:
                    url = template.format(
                        i=rating_val,
                        j=page_val,
                        rating=rating_val,
                        page=page_val,
                    )
                except Exception as e:
                    print(
                        f"[ERROR] Failed to format template for rating={rating} page={page}: {e}"
                    )
                    stopped_for_rating = True
                    break

                _, reviews = await fetch_and_extract_async(
                    url, args.timeout, args.max_retries
                )
                # annotate rating value for records
                for r in reviews:
                    r.rating = rating_val
                # Keep only rows where we have at least a name or a review
                reviews = [r for r in reviews if r.name or r.review]
                if len(reviews) == 0:
                    # If we are not ignoring empty pages, stop the inner page loop; otherwise, just continue
                    if not args.ignore_empty_pages:
                        print(
                            f"[INFO] No reviews returned for {url}; stopping this rating and moving to next rating."
                        )
                        stopped_for_rating = True
                        break
                    else:
                        print(
                            f"[INFO] No reviews returned for {url}; --ignore-empty-pages enabled, continuing to next page."
                        )
                all_records.extend(reviews)

            if stopped_for_rating:
                # Continue to next rating
                continue

    elif args.mode == "urls":
        # If no explicit URLs are provided, build URLs from the Shopify template and i/j ranges.
        # Template: https://apps.shopify.com/{name_of_company}/reviews?ratings%5B%5D={i}&page={j}
        if not args.urls:
            # Default supports both placeholder schemes but prefer {rating}/{page} names
            template = f"https://apps.shopify.com/{args.company}/reviews?ratings%5B%5D={{rating}}&page={{page}}"
            # Use the same nested loops as in 'loops' mode so the "break inner loop on zero" behavior
            # is applied here when building URLs programmatically
            zero_pad_i = args.zero_pad_i
            zero_pad_j = args.zero_pad_j
            for rating in range(args.ratings_start, args.ratings_end):
                if zero_pad_i and isinstance(rating, int):
                    rating_val = str(rating).zfill(zero_pad_i)
                else:
                    rating_val = str(rating)

                stopped_for_rating = False
                # Determine page iterable. If page_end is None, iterate until no reviews.
                if args.page_end is None:
                    page_iter = itertools.count(args.page_start)
                else:
                    page_iter = range(args.page_start, args.page_end)
                pages_checked = 0
                for page in page_iter:
                    pages_checked += 1
                    if args.max_pages is not None and pages_checked > args.max_pages:
                        print(
                            f"[INFO] Reached --max-pages={args.max_pages} for rating={rating}; stopping page loop."
                        )
                        stopped_for_rating = True
                        break
                    if zero_pad_j and isinstance(page, int):
                        page_val = str(page).zfill(zero_pad_j)
                    else:
                        page_val = str(page)

                    try:
                        url = template.format(
                            i=rating_val,
                            j=page_val,
                            rating=rating_val,
                            page=page_val,
                        )
                    except Exception as e:
                        print(
                            f"[ERROR] Failed to format template for rating={rating} page={page}: {e}"
                        )
                        stopped_for_rating = True
                        break

                    _, reviews = await fetch_and_extract_async(
                        url, args.timeout, args.max_retries
                    )
                    reviews = [r for r in reviews if r.name or r.review]
                    if len(reviews) == 0:
                        consecutive_zero_pages += 1
                        if not args.ignore_empty_pages:
                            print(
                                f"[INFO] No reviews returned for {url}; stopping this rating and moving to next rating."
                            )
                            stopped_for_rating = True
                            break
                        if consecutive_zero_pages >= args.zero_page_threshold:
                            print(
                                f"[INFO] Reached {consecutive_zero_pages} consecutive zero-review pages for rating={rating}; skipping to next rating."
                            )
                            stopped_for_rating = True
                            break
                        print(
                            f"[INFO] No reviews returned for {url}; --ignore-empty-pages enabled, continuing to next page."
                        )
                    else:
                        consecutive_zero_pages = 0
                        all_records.extend(reviews)
                if stopped_for_rating:
                    continue
        else:
            urls = args.urls
            for url in urls:
                _, reviews = await fetch_and_extract_async(
                    url, args.timeout, args.max_retries
                )
                # If we can, parse rating from the URL and set it on results
                try:
                    parsed = urllib.parse.urlparse(url)
                    qs = urllib.parse.parse_qs(parsed.query)
                    rating_val = ""
                    # look for 'ratings[]' or 'ratings%5B%5D' or 'rating'
                    for key in ("ratings[]", "ratings%5B%5D", "rating", "ratings"):
                        if key in qs and qs[key]:
                            rating_val = qs[key][0]
                            break
                except Exception:
                    rating_val = ""
                for r in reviews:
                    r.rating = rating_val
                    for r in reviews:
                        r.rating = rating_val
                reviews = [r for r in reviews if r.name or r.review]
                if len(reviews) == 0:
                    print(
                        f"[INFO] No reviews returned for {url}; stopping further requests."
                    )
                    break
                all_records.extend(reviews)

    # monitor task already started earlier; ensure we don't start twice

    # out_path = Path(args.output)
    # write_aggregated_csv(all_records, out_path)
    # Stop monitor task if it was started
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    main()
