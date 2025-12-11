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
