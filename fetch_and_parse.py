#!/usr/bin/env python3
"""
fetch_and_parse.py

Unified script that:
  1) Uses Playwright to open a Luma page and capture all calendar API responses while scrolling.
  2) Parses the captured responses and writes a single JSON file with the requested
     event fields (name, full URL, geo info, latitude, longitude).

The script is organized into well-defined functions with comments. It will fail
gracefully with clear instructions if Playwright is not installed.

Usage examples:
  # capture + parse, removing the intermediate capture file
  python fetch_and_parse.py --url https://luma.com/devconnect --output events_all_extracted.json

  # keep the raw Playwright capture for debugging
  python fetch_and_parse.py --keep-capture --capture all_responses.json --output events.json

Requirements:
  pip install playwright
  playwright install

"""

from __future__ import annotations

import argparse
import json
import time
import os
import sys
from typing import Any, Dict, List, Optional, Tuple


API_URL_PART = "api2.luma.com/calendar/get"
DEFAULT_CAPTURE = "all_responses.json"
DEFAULT_OUTPUT = "events_all_extracted.json"
BASE_EVENT_URL = "https://luma.com/"


def capture_with_playwright(url: str, capture_path: str, timeout_idle: int = 30, headless: bool = True) -> int:
    """Open `url` with Playwright, scroll until no new calendar API responses are seen
    for `timeout_idle` seconds, and save captured responses to `capture_path`.

    Returns the number of captured responses.
    Raises RuntimeError with instructions if Playwright is not available.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is not installed. Install it with: `pip install playwright` and run `playwright install`"
        ) from exc

    captured: List[Dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        def on_response(resp):
            try:
                if API_URL_PART in resp.url:
                    # attempt to read JSON body, fall back to text
                    try:
                        body = resp.json()
                    except Exception:
                        try:
                            body = json.loads(resp.text())
                        except Exception:
                            body = {"raw_text": resp.text()}
                    captured.append({"url": resp.url, "status": resp.status, "body": body})
            except Exception:
                # ignore errors while capturing a response
                pass

        page.on("response", on_response)

        page.goto(url)

        last_count = 0
        idle_since = time.time()
        # Scroll repeatedly until we see no new captures for timeout_idle seconds
        while True:
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(1)
            if len(captured) > last_count:
                last_count = len(captured)
                idle_since = time.time()
            if time.time() - idle_since > timeout_idle:
                break

        browser.close()

    # write capture to file
    with open(capture_path, "w", encoding="utf-8") as f:
        json.dump(captured, f, ensure_ascii=False, indent=2)

    return len(captured)


def find_event_dicts(obj: Any) -> List[Dict[str, Any]]:
    """Recursively find dicts that contain an 'event' key mapping to a dict."""
    found: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if "event" in obj and isinstance(obj["event"], dict):
            found.append(obj)
        for v in obj.values():
            found.extend(find_event_dicts(v))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(find_event_dicts(item))
    return found


def extract_event_fields(record: Dict[str, Any]) -> Dict[str, Optional[Any]]:
    """Extract the requested fields from a single record containing `event`."""
    ev = record.get("event", {}) or {}
    geo = ev.get("geo_address_info") or {}
    coord = ev.get("coordinate") or {}
    ticket = record.get("ticket_info") or ev.get("ticket_info") or {}

    url_val = ev.get("url")
    full_url = None
    if url_val:
        if url_val.startswith("/"):
            url_val = url_val[1:]
        full_url = BASE_EVENT_URL + url_val

    return {
        "name": ev.get("name"),
        "url": full_url,
        # -- full geo_address_info fields
        "geo_address_info_city": geo.get("city"),
        "geo_address_info_type": geo.get("type"),
        "geo_address_info_region": geo.get("region"),
        "geo_address_info_address": geo.get("address"),
        "geo_address_info_country": geo.get("country"),
        "geo_address_info_place_id": geo.get("place_id"),
        "geo_address_info_city_state": geo.get("city_state"),
        "geo_address_info_description": geo.get("description"),
        "geo_address_info_country_code": geo.get("country_code"),
        "geo_address_info_full_address": geo.get("full_address"),
        "geo_address_info_apple_maps_place_id": geo.get("apple_maps_place_id"),
        "geo_address_info_mode": geo.get("mode"),
        "geo_address_visibility": ev.get("geo_address_visibility"),
        "latitude": coord.get("latitude"),
        "longitude": coord.get("longitude"),
        # Ticket info
        "ticket_is_free": (ticket.get("is_free") if isinstance(ticket, dict) else None),
        "ticket_price_usd": (None if ticket.get("is_free") else (
            (ticket.get("price") or {}).get("cents") / 100.0 if (ticket.get("price") or {}).get("cents") is not None else None
        )),
        "ticket_require_approval": (ticket.get("require_approval") if isinstance(ticket, dict) else None),
        "ticket_is_sold_out": (ticket.get("is_sold_out") if isinstance(ticket, dict) else None),
        "ticket_count": record.get("ticket_count"),
        "guest_count": record.get("guest_count"),
        "ticket_max_price": (ticket.get("max_price") if isinstance(ticket, dict) else None),
        "ticket_spots_remaining": (ticket.get("spots_remaining") if isinstance(ticket, dict) else None),
        "ticket_is_near_capacity": (ticket.get("is_near_capacity") if isinstance(ticket, dict) else None),
        "ticket_currency_info": (ticket.get("currency_info") if isinstance(ticket, dict) else None),
        "waitlist_enabled": ev.get("waitlist_enabled"),
    }


def parse_capture_to_events(capture_path: str) -> Tuple[int, List[Dict[str, Optional[Any]]]]:
    """Read a Playwright capture file and extract event fields from all captured responses.

    Returns (num_captures, list_of_event_dicts).
    """
    with open(capture_path, "r", encoding="utf-8") as f:
        captures = json.load(f)

    all_results: List[Dict[str, Optional[Any]]] = []
    for cap in captures:
        body = cap.get("body")
        if not body:
            continue
        records = find_event_dicts(body)
        for rec in records:
            all_results.append(extract_event_fields(rec))

    return len(captures), all_results


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Capture Luma calendar responses and extract event fields")
    p.add_argument("--url", default="https://luma.com/devconnect")
    p.add_argument("--capture", default=DEFAULT_CAPTURE, help="Temporary capture file path")
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="Final extracted events JSON file")
    p.add_argument("--timeout", type=int, default=30, help="Seconds of idle time to stop scrolling")
    p.add_argument("--headless", action="store_true", help="Run browser in headless mode (default: true)")
    p.add_argument("--keep-capture", action="store_true", help="Keep the intermediate capture file")
    args = p.parse_args(argv)

    # Step 1: capture
    try:
        print(f"Starting Playwright capture of {args.url} -> {args.capture} (idle timeout {args.timeout}s)")
        num_captures = capture_with_playwright(args.url, args.capture, timeout_idle=args.timeout, headless=not args.headless)
        print(f"Captured {num_captures} responses to {args.capture}")
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 3

    # Step 2: parse capture and write final output
    num_caps, events = parse_capture_to_events(args.capture)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print(f"Extracted {len(events)} event records across {num_caps} captures -> {args.output}")

    # Optionally remove capture file
    if not args.keep_capture:
        try:
            os.remove(args.capture)
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
