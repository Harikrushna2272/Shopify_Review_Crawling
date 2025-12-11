# import asyncio
# import io
# import itertools
# from pathlib import Path
# from typing import List, Tuple
# import urllib.parse

# import streamlit as st

# # Local imports from the project
# # These modules exist in the same 'review_scrapping' package
# from extract_reviews_to_csv import ReviewRecord, write_csv, extract_reviews
# from html_scraper import fetch_html_sync, fetch_html_async
# from main import fetch_and_extract_async as main_fetch_and_extract_async
# # We reuse parts of the logic in main.py to mirror the loops mode flow.


# def fetch_and_extract_sync(
#     url: str, timeout: float, max_retries: int
# ) -> Tuple[str, List[ReviewRecord]]:
#     """
#     Fetch a single URL synchronously and extract reviews from its HTML.
#     """
#     page = fetch_html_sync(
#         url=url,
#         timeout=timeout,
#         max_retries=max_retries,
#         backoff_factor=0.5,
#         headers=None,
#         follow_redirects=True,
#         verify_ssl=True,
#         raise_for_status=False,
#         encoding=None,
#     )
#     if getattr(page, "error", None):
#         st.write(f"[WARN] Failed to fetch {url}: {page.error}")
#         return url, []

#     html_text = page.html or ""
#     if not html_text.strip():
#         st.write(f"[WARN] Empty HTML from {url}")
#         return url, []

#     reviews = extract_reviews(html_text)
#     st.write(f"[INFO] {url}: extracted {len(reviews)} reviews")
#     return url, reviews


# async def fetch_and_extract_async(
#     url: str, timeout: float, max_retries: int
# ) -> Tuple[str, List[ReviewRecord]]:
#     """
#     Delegate to main.py's async fetch-and-extract to keep flow identical.
#     """
#     url, reviews = await main_fetch_and_extract_async(url, timeout, max_retries)
#     st.write(f"[INFO] {url}: extracted {len(reviews)} reviews (async)")
#     return url, reviews


# async def scrape_shopify_reviews(
#     company: str,
#     ratings_start: int,
#     ratings_end: int,
#     page_start: int = 1,
#     timeout: float = 30.0,
#     max_retries: int = 4,
#     ignore_empty_pages: bool = False,
#     zero_page_threshold: int = 5,
#     use_async_fetch: bool = True,
# ) -> List[ReviewRecord]:
#     """
#     Scrape Shopify reviews using the same template pattern as the CLI:
#     https://apps.shopify.com/{company}/reviews?ratings%5B%5D={rating}&page={page}

#     - Iterates ratings in [ratings_start, ratings_end)
#     - For each rating, iterates pages starting at page_start
#     - Stops page iteration for a rating when a page returns zero reviews
#       (unless ignore_empty_pages=True; then continues until zero_page_threshold is reached)
#     """
#     all_records: List[ReviewRecord] = []

#     template = f"https://apps.shopify.com/{company}/reviews?ratings%5B%5D={{rating}}&page={{page}}"

#     # Progress bars
#     total_ratings = max(0, ratings_end - ratings_start)
#     rating_progress = st.progress(0, text="Starting ratings loop...")
#     page_progress = st.progress(0, text="Waiting for first page...")

#     rating_index = 0
#     for rating in range(ratings_start, ratings_end):
#         consecutive_zero_pages = 0
#         pages_checked = 0
#         rating_index += 1
#         rating_progress.progress(
#             min(1.0, rating_index / max(1, total_ratings)),
#             text=f"Processing rating {rating} ({rating_index}/{total_ratings})",
#         )

#         # Iterate pages until zero reviews
#         page_iter = itertools.count(page_start)
#         for page in page_iter:
#             pages_checked += 1
#             page_progress.progress(
#                 min(1.0, (pages_checked % 100) / 100.0),  # just a moving indicator
#                 text=f"Rating {rating}: fetching page {page}",
#             )
#             # Rate-limit sleep to mirror main.py loops mode behavior
#             if pages_checked % 22 == 0:
#                 st.write(
#                     f"[INFO] Fetched {pages_checked} pages for rating={rating}; sleeping for 8s to avoid rate limits."
#                 )
#                 await asyncio.sleep(8)

#             url = template.format(rating=rating, page=page)

#             if use_async_fetch:
#                 _, reviews = await fetch_and_extract_async(url, timeout, max_retries)
#             else:
#                 _, reviews = fetch_and_extract_sync(url, timeout, max_retries)

#             # annotate rating value on records
#             rating_val = str(rating)
#             for r in reviews:
#                 r.rating = rating_val

#             # Keep only rows with at least a name or a review
#             reviews = [r for r in reviews if r.name or r.review]

#             if len(reviews) == 0:
#                 consecutive_zero_pages += 1
#                 st.write(
#                     f"[INFO] No reviews for {url} (consecutive zero pages: {consecutive_zero_pages})"
#                 )
#                 if (
#                     not ignore_empty_pages
#                     or consecutive_zero_pages >= zero_page_threshold
#                 ):
#                     st.write(
#                         f"[INFO] Stopping rating {rating} after {pages_checked} pages."
#                     )
#                     break
#                 else:
#                     continue
#             else:
#                 consecutive_zero_pages = 0
#                 all_records.extend(reviews)

#         # move to next rating after stopping the inner loop
#         continue

#     rating_progress.progress(1.0, text="Done processing ratings.")
#     page_progress.progress(1.0, text="Done.")
#     return all_records


# def records_to_csv_bytes(records: List[ReviewRecord]) -> bytes:
#     """
#     Write records to CSV into an in-memory buffer and return bytes for download.
#     """
#     buf = io.StringIO()
#     buf_writer = io.TextIOWrapper(
#         io.BytesIO(), encoding="utf-8", write_through=True
#     )  # helper not used directly, keeping simple below

#     # Manual CSV writing using the same header as write_csv
#     # We will reuse csv module for robust quoting
#     import csv

#     output = io.StringIO()
#     writer = csv.writer(output)
#     writer.writerow(["rating", "name", "review", "date", "location", "usage_duration"])
#     for rec in records:
#         writer.writerow(
#             [
#                 rec.rating,
#                 rec.name,
#                 rec.review,
#                 rec.date,
#                 rec.location,
#                 rec.usage_duration,
#             ]
#         )

#     return output.getvalue().encode("utf-8")


# def main_ui():
#     st.set_page_config(
#         page_title="Shopify Reviews Scraper", page_icon="🛒", layout="centered"
#     )
#     st.title("Shopify App Reviews Scraper")
#     st.write(
#         "Use this UI to scrape reviews for a Shopify app from the public reviews pages. "
#         "It mirrors the CLI behavior and lets you download the results as a CSV."
#     )

#     # Inputs (only the 3 required fields)
#     company = st.text_input(
#         "Company (Shopify app slug)",
#         value="tiktok",
#         help="Example: 'tiktok' for apps.shopify.com/tiktok",
#     )
#     ratings_col1, ratings_col2 = st.columns(2)
#     with ratings_col1:
#         ratings_start = st.number_input(
#             "Ratings start (inclusive)", min_value=1, max_value=10, value=1, step=1
#         )
#     with ratings_col2:
#         ratings_end = st.number_input(
#             "Ratings end (exclusive)",
#             min_value=2,
#             max_value=11,
#             value=6,
#             step=1,
#             help="Default covers 1..5",
#         )

#     run_btn = st.button("Run scraper")

#     if run_btn:
#         if not company.strip():
#             st.error("Company cannot be empty.")
#             return
#         if ratings_end <= ratings_start:
#             st.error("Ratings end must be greater than ratings start.")
#             return

#         st.info(
#             f"Starting scrape: company={company}, ratings={ratings_start}..{ratings_end - 1}"
#         )

#         # Build args object to mirror the CLI values and pass into main.main_async
#         from dataclasses import dataclass
#         from typing import Optional, List

#         @dataclass
#         class Args:
#             mode: str
#             company: str
#             ratings_start: int
#             ratings_end: int
#             page_start: int = 1
#             page_end: Optional[int] = None
#             ignore_empty_pages: bool = False
#             max_pages: Optional[int] = 20  # halt after 20 pages per rating
#             zero_page_threshold: int = 5
#             timeout: float = 30.0
#             max_retries: int = 4
#             output: str = ""
#             monitor: bool = True
#             monitor_interval: int = 10
#             template: Optional[str] = None
#             urls: List[str] = None
#             # legacy fields kept for completeness
#             i_start: Optional[int] = None
#             i_end: Optional[int] = None
#             j_start: Optional[int] = None
#             j_end: Optional[int] = None
#             zero_pad_i: Optional[int] = None
#             zero_pad_j: Optional[int] = None

#         args = Args(
#             mode="loops",
#             company=company.strip(),
#             ratings_start=int(ratings_start),
#             ratings_end=int(ratings_end),
#             page_start=1,
#             page_end=None,
#             ignore_empty_pages=False,
#             max_pages=20,
#             zero_page_threshold=5,
#             timeout=30.0,
#             max_retries=4,
#             output=str(Path("outputs") / f"{company.strip()}_reviews.csv"),
#             monitor=True,
#             monitor_interval=10,
#             template=None,
#             urls=[],
#         )

#         # Run the same async pipeline as main.py
#         from main import main_async as cli_main_async

#         records: List[ReviewRecord] = []

#         async def run_pipeline_collect():
#             await cli_main_async(args)
#             # The CLI main writes aggregated CSV to args.output.
#             # To provide a download, read back the CSV and reconstruct records for preview.
#             try:
#                 import csv

#                 out_path = Path(args.output)
#                 if out_path.exists():
#                     rows = []
#                     with out_path.open("r", encoding="utf-8", newline="") as f:
#                         reader = csv.DictReader(f)
#                         for row in reader:
#                             rec = ReviewRecord(
#                                 name=row.get("name", "") or "",
#                                 review=row.get("review", "") or "",
#                                 date=row.get("date", "") or "",
#                                 location=row.get("location", "") or "",
#                                 usage_duration=row.get("usage_duration", "") or "",
#                                 rating=row.get("rating", "") or "",
#                             )
#                             rows.append(rec)
#                     return rows
#             except Exception:
#                 pass
#             return []

#         records = asyncio.run(run_pipeline_collect())

#         st.success(f"Scrape completed. Extracted {len(records)} reviews.")
#         if len(records) == 0:
#             st.warning("No reviews found with the given parameters.")

#         # Prepare CSV for download (use the file written by main.py if present; else fall back to in-memory)
#         default_output_name = f"outputs/{company}_reviews.csv"
#         out_path = Path(default_output_name)
#         if out_path.exists():
#             st.download_button(
#                 label="Download CSV",
#                 data=out_path.read_bytes(),
#                 file_name=out_path.name,
#                 mime="text/csv",
#             )
#         else:
#             csv_bytes = records_to_csv_bytes(records)
#             st.download_button(
#                 label="Download CSV",
#                 data=csv_bytes,
#                 file_name=Path(default_output_name).name,
#                 mime="text/csv",
#             )

#         # Optional: show a sample of the results
#         if records:
#             st.subheader("Sample results")
#             sample_count = min(10, len(records))
#             sample_rows = [
#                 {
#                     "rating": r.rating,
#                     "name": r.name,
#                     "review": r.review[:300],
#                     "date": r.date,
#                     "location": r.location,
#                     "usage_duration": r.usage_duration,
#                 }
#                 for r in records[:sample_count]
#             ]
#             st.dataframe(sample_rows, use_container_width=True)


# if __name__ == "__main__":
#     main_ui()


import asyncio
import itertools
import io
from pathlib import Path
from typing import List, Tuple
import csv

import streamlit as st

# Import from your project modules
from extract_reviews_to_csv import ReviewRecord
from main import (
    fetch_and_extract_async,
    write_aggregated_csv,
    monitor_stats,
)


def run_scraper_ui(company: str, ratings_start: int, ratings_end: int):
    """
    UI-driven loops-mode scraper that mirrors main.py's logic:
    - ratings in [ratings_start, ratings_end)
    - pages from 1 upwards
    - break on zero reviews
    - sleep 8s after every 22 pages
    - monitor with interval 10
    """
    logs_buf = io.StringIO()
    all_records: List[ReviewRecord] = []

    async def pipeline():
        monitor_task = asyncio.create_task(monitor_stats(10))
        try:
            template = f"https://apps.shopify.com/{company}/reviews?ratings%5B%5D={{rating}}&page={{page}}"

            for rating in range(ratings_start, ratings_end):
                pages_checked = 0
                for page in itertools.count(1):
                    pages_checked += 1

                    # Rate-limit sleep (exactly like main.py)
                    if pages_checked % 22 == 0:
                        msg = f"[INFO] Fetched {pages_checked} pages for rating={rating}; sleeping for 8s to avoid rate limits."
                        print(msg)
                        logs_buf.write(msg + "\n")
                        await asyncio.sleep(8)

                    url = template.format(rating=rating, page=page)

                    _, reviews = await fetch_and_extract_async(
                        url=url, timeout=30.0, max_retries=4
                    )
                    # annotate rating
                    for r in reviews:
                        r.rating = str(rating)
                    # filter empty rows (same rule used elsewhere)
                    reviews = [r for r in reviews if r.name or r.review]

                    if len(reviews) == 0:
                        info = f"[INFO] No reviews returned for {url}; stopping this rating and moving to next rating."
                        print(info)
                        logs_buf.write(info + "\n")
                        break

                    all_records.extend(reviews)
        finally:
            # Stop monitor
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    # Run the async pipeline
    asyncio.run(pipeline())

    # Write aggregated CSV to outputs/{company}_reviews.csv
    out_path = Path("outputs") / f"{company}_reviews.csv"
    write_aggregated_csv(all_records, out_path)

    return all_records, logs_buf.getvalue(), out_path


def main_ui():
    st.set_page_config(page_title="Shopify Reviews Scraper", layout="centered")
    st.title("Shopify App Reviews Scraper")

    st.caption(
        "Enter the Shopify app handle and rating range. The scraper will run with defaults "
        "(page-start=1, monitor-interval=10) and provide a CSV to download when done."
    )

    # Only the 3 inputs you requested
    company = st.text_input(
        "Company (Shopify app handle)", value="tiktok", help="apps.shopify.com/<handle>"
    )
    col1, col2 = st.columns(2)
    with col1:
        ratings_start = st.number_input(
            "Ratings start (inclusive)", min_value=1, max_value=5, value=1, step=1
        )
    with col2:
        ratings_end_exclusive = st.number_input(
            "Ratings end (exclusive)", min_value=2, max_value=6, value=6, step=1
        )

    run_btn = st.button("Run scraper")

    if run_btn:
        if not company.strip():
            st.error("Company cannot be empty.")
            return
        if ratings_end_exclusive <= ratings_start:
            st.error("Ratings end must be greater than ratings start.")
            return

        with st.spinner("Scraping in progress..."):
            records, logs, out_path = run_scraper_ui(
                company=company.strip(),
                ratings_start=int(ratings_start),
                ratings_end=int(ratings_end_exclusive),
            )

        st.success(f"Scrape completed. Extracted {len(records)} reviews.")
        st.subheader("Run logs")
        st.text_area("Logs", logs, height=300)

        # Download the CSV written by the pipeline
        if out_path.exists():
            st.download_button(
                label=f"Download CSV ({out_path.name})",
                data=out_path.read_bytes(),
                file_name=out_path.name,
                mime="text/csv",
            )

        # Show a small preview
        if records:
            st.subheader("Preview (first 10 rows)")
            sample = records[:10]
            rows = [
                {
                    "rating": r.rating,
                    "name": r.name,
                    "review": r.review[:300],
                    "date": r.date,
                    "location": r.location,
                    "usage_duration": r.usage_duration,
                }
                for r in sample
            ]
            st.dataframe(rows, use_container_width=True)


if __name__ == "__main__":
    main_ui()
