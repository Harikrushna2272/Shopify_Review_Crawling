from __future__ import annotations

import argparse
import csv
import html
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass
class ReviewRecord:
    name: str
    review: str
    date: str
    location: str
    usage_duration: str
    rating: str = ""


def _compile_patterns():
    # Review text: <p class="tw-break-words">...</p>
    # Review container: div that wraps multiple review paragraphs
    container_re = re.compile(
        r"<div[^>]*\bdata-truncate-content-copy\b[^>]*>(.*?)</div>",
        re.IGNORECASE | re.DOTALL,
    )
    # Individual review paragraph inside the container
    paragraph_re = re.compile(
        r'<p\s+class="[^"]*\btw-break-words\b[^"]*">\s*(.*?)\s*</p>',
        re.IGNORECASE | re.DOTALL,
    )

    # Name: <span class="...tw-overflow-hidden...tw-text-ellipsis...tw-whitespace-nowrap..." title="NAME">NAME</span>
    # We try to be robust: title attribute is optional; inner text as fallback.
    name_re = re.compile(
        r'<span\s+class="[^"]*\btw-overflow-hidden\b[^"]*\btw-text-ellipsis\b[^"]*\btw-whitespace-nowrap\b[^"]*"'
        r'[^>]*?(?:\s+title="([^"]*)")?[^>]*>\s*(.*?)\s*</span>',
        re.IGNORECASE | re.DOTALL,
    )

    # Date: <div class="tw-text-body-xs tw-text-fg-tertiary">DATE</div>
    date_re = re.compile(
        r'<div\s+class="[^"]*\btw-text-body-xs\b[^"]*\btw-text-fg-tertiary\b[^"]*">\s*(.*?)\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )

    # Combined Location + Usage:
    # ... <div>United States</div> <div>4 days using the app</div> ...
    loc_usage_re = re.compile(
        r"<div>\s*([A-Za-z][^<>]{1,120}?)\s*</div>\s*<div>\s*([A-Za-z0-9 ,.\-]{1,140}?\s+using the app)\s*</div>",
        re.IGNORECASE | re.DOTALL,
    )

    # Usage only:
    usage_re = re.compile(
        r"<div>\s*([A-Za-z0-9 ,.\-]{1,140}?\s+using the app)\s*</div>",
        re.IGNORECASE | re.DOTALL,
    )

    # A simple div text capture (no attributes) for neighbor lookup
    simple_div_text_re = re.compile(
        r"<div>\s*([^<>]{1,120}?)\s*</div>", re.IGNORECASE | re.DOTALL
    )

    return {
        "container": container_re,
        "paragraph": paragraph_re,
        "name": name_re,
        "date": date_re,
        "loc_usage": loc_usage_re,
        "usage": usage_re,
        "simple_div": simple_div_text_re,
    }


def _strip_tags(text: str) -> str:
    # Replace common break tags with spaces before stripping
    text = re.sub(r"<\s*br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/p\s*>", " ", text, flags=re.IGNORECASE)
    # Remove all other tags
    text = re.sub(r"<[^>]+>", "", text)
    # Unescape entities and normalize whitespace
    text = html.unescape(text)
    text = " ".join(text.split())
    return text.strip()


def _find_last_match_before(pattern: re.Pattern, haystack: str, end_index: int):
    """
    Find the last pattern match whose end() index is <= end_index.
    Returns Match or None.
    """
    last = None
    for m in pattern.finditer(haystack):
        if m.end() <= end_index:
            last = m
        else:
            break
    return last


def _extract_loc_usage_in_window(
    window: str,
    review_local_start: int,
    patterns: dict,
) -> Tuple[str, str]:
    """
    Try to extract location and usage from window text near the review.
    Strategy:
      1) Find the last combined (location, usage) match before the review start.
      2) Else, find the first combined match after the review start.
      3) Else, find a usage-only match nearest before the review start and try to
         pair it with the preceding simple <div> text as location.
    """
    loc_usage_re = patterns["loc_usage"]
    usage_re = patterns["usage"]
    simple_div_re = patterns["simple_div"]

    # 1) Combined, last before review start
    last_before = None
    for m in loc_usage_re.finditer(window):
        if m.end() <= review_local_start:
            last_before = m
        else:
            break
    if last_before:
        loc = _strip_tags(last_before.group(1))
        usage = _strip_tags(last_before.group(2))
        return loc, usage

    # 2) Combined, first after review start
    first_after = None
    for m in loc_usage_re.finditer(window):
        if m.start() >= review_local_start:
            first_after = m
            break
    if first_after:
        loc = _strip_tags(first_after.group(1))
        usage = _strip_tags(first_after.group(2))
        return loc, usage

    # 3) Usage-only nearest before review start + previous simple div as location
    nearest_usage_before = _find_last_match_before(usage_re, window, review_local_start)
    if nearest_usage_before:
        usage = _strip_tags(nearest_usage_before.group(1))
        # find previous simple <div> text before usage start
        up_to_usage = window[: nearest_usage_before.start()]
        # get the last simple-div text from this truncated window
        last_simple = None
        for d in simple_div_re.finditer(up_to_usage):
            last_simple = d
        loc = _strip_tags(last_simple.group(1)) if last_simple else ""
        return loc, usage

    # If still not found, attempt a usage-only after as a last resort (no location)
    for m in usage_re.finditer(window):
        if m.start() >= review_local_start:
            usage = _strip_tags(m.group(1))
            return "", usage

    return "", ""


def extract_reviews(html_text: str) -> List[ReviewRecord]:
    patterns = _compile_patterns()

    container_re = patterns["container"]
    paragraph_re = patterns["paragraph"]
    name_re = patterns["name"]
    date_re = patterns["date"]

    # Heuristic window around each review paragraph, to find neighbors
    # Widened window to better capture nearby name/date/location across different pages
    window_radius = 5000

    records: List[ReviewRecord] = []

    for cmatch in container_re.finditer(html_text):
        container_html = cmatch.group(1)
        # Collect and aggregate all paragraphs within this container
        paragraphs = []
        first_p_local_start = None
        for pm in paragraph_re.finditer(container_html):
            if first_p_local_start is None:
                first_p_local_start = pm.start()
            paragraphs.append(_strip_tags(pm.group(1)))
        if not paragraphs:
            # Fallback: if no paragraphs found, skip this container
            continue
        review_text = "\n\n".join(p for p in paragraphs if p)

        # Determine window bounds
        # Determine window bounds using the position of the first paragraph within the container
        first_p_global_start = cmatch.start() + (first_p_local_start or 0)
        start_idx = max(0, first_p_global_start - window_radius)
        end_idx = min(len(html_text), first_p_global_start + window_radius)
        window = html_text[start_idx:end_idx]
        local_review_start = first_p_global_start - start_idx

        # Name: choose the nearest name match before or after the review paragraph in the window
        name = ""
        last_name_before = _find_last_match_before(name_re, window, local_review_start)

        first_name_after = None
        for m in name_re.finditer(window):
            if m.start() >= local_review_start:
                first_name_after = m
                break

        chosen_name = None
        if last_name_before and first_name_after:
            dist_before = local_review_start - last_name_before.end()
            dist_after = first_name_after.start() - local_review_start
            chosen_name = (
                last_name_before if dist_before <= dist_after else first_name_after
            )
        else:
            chosen_name = last_name_before or first_name_after

        if chosen_name:
            # Prefer title attribute if present, else inner text
            title_val = chosen_name.group(1) or ""
            inner_text = _strip_tags(chosen_name.group(2) or "")
            name = title_val.strip() or inner_text.strip()

        # Date: last date match before the review
        date = ""
        last_date_before = _find_last_match_before(date_re, window, local_review_start)
        if last_date_before:
            date = _strip_tags(last_date_before.group(1))

        # Location and usage
        location, usage = _extract_loc_usage_in_window(
            window, local_review_start, patterns
        )

        records.append(
            ReviewRecord(
                name=name,
                review=review_text,
                date=date,
                location=location,
                usage_duration=usage,
            )
        )

    return records


def write_csv(records: List[ReviewRecord], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["rating", "name", "review", "date", "location", "usage_duration"]
        )
        for rec in records:
            writer.writerow(
                [
                    rec.rating,
                    rec.name,
                    rec.review,
                    rec.date,
                    rec.location,
                    rec.usage_duration,
                ]
            )


def main():
    parser = argparse.ArgumentParser(
        description="Extract Shopify TikTok reviews from HTML and write to CSV (regex-based)."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(
            Path(__file__).resolve().parents[2]
            / "outputs"
            / "shopify_tiktok_reviews_page_1.html"
        ),
        help="Path to the input HTML file (default: outputs/shopify_tiktok_reviews_page_1.html)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(
            Path(__file__).resolve().parents[2]
            / "outputs"
            / "shopify_tiktok_reviews_page_1.csv"
        ),
        help="Path to the CSV output file (default: outputs/shopify_tiktok_reviews_page_1.csv)",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        raise FileNotFoundError(f"Input HTML not found: {in_path}")

    html_text = in_path.read_text(encoding="utf-8", errors="ignore")
    records = extract_reviews(html_text)

    # Optional: Filter out empty rows where both review and name are missing
    records = [r for r in records if r.review or r.name]

    write_csv(records, out_path)

    print(f"Extracted {len(records)} reviews to: {out_path}")


if __name__ == "__main__":
    main()
