#!/usr/bin/env python3
"""
Collin County, Texas — Motivated Seller Lead Scraper
Clerk portal: https://collin.tx.publicsearch.us/
Parcel data:  https://collincad.org/open-data-portal/
Look-back:    last 7 days
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dbfread import DBF
from playwright.async_api import async_playwright, Page, BrowserContext

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("collin_scraper")

# ─── Constants ────────────────────────────────────────────────────────────────
CLERK_BASE = "https://collin.tx.publicsearch.us"
CAD_PORTAL = "https://collincad.org/open-data-portal/"
LOOKBACK_DAYS = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY = 4  # seconds between retries

# Document-type → (category code, human label, base flags)
DOC_TYPE_MAP: dict[str, tuple[str, str, list[str]]] = {
    # Lis Pendens
    "LP":      ("LP",     "Lis Pendens",             ["Lis pendens", "Pre-foreclosure"]),
    "RELLP":   ("RELLP",  "Release Lis Pendens",      ["Lis pendens"]),
    # Foreclosure
    "NOFC":    ("NOFC",   "Notice of Foreclosure",    ["Pre-foreclosure"]),
    # Tax Deed
    "TAXDEED": ("TAXDEED","Tax Deed",                 ["Tax lien"]),
    # Judgments
    "JUD":     ("JUD",    "Judgment",                 ["Judgment lien"]),
    "CCJ":     ("CCJ",    "Certified Judgment",       ["Judgment lien"]),
    "DRJUD":   ("DRJUD",  "Domestic Judgment",        ["Judgment lien"]),
    # Liens – corporate / federal
    "LNCORPTX":("LNCORPTX","Corp Tax Lien",           ["Tax lien"]),
    "LNIRS":   ("LNIRS",  "IRS Lien",                 ["Tax lien"]),
    "LNFED":   ("LNFED",  "Federal Lien",             ["Tax lien"]),
    # Liens – general
    "LN":      ("LN",     "Lien",                     ["Mechanic lien"]),
    "LNMECH":  ("LNMECH", "Mechanic Lien",            ["Mechanic lien"]),
    "LNHOA":   ("LNHOA",  "HOA Lien",                 ["Mechanic lien"]),
    "MEDLN":   ("MEDLN",  "Medicaid Lien",            ["Judgment lien"]),
    # Probate
    "PRO":     ("PRO",    "Probate Document",         ["Probate / estate"]),
    # Notice
    "NOC":     ("NOC",    "Notice of Commencement",   []),
}

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR = ROOT / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def retry(fn):
    """Simple synchronous retry decorator."""
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                log.warning("Attempt %d/%d failed for %s: %s",
                            attempt, RETRY_ATTEMPTS, fn.__name__, exc)
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(RETRY_DELAY * attempt)
        raise last_exc
    return wrapper


def safe_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def parse_amount(raw: str) -> float | None:
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def name_variants(full_name: str) -> list[str]:
    """Return lookup variants for a name string."""
    full_name = full_name.strip()
    variants = {full_name.upper()}
    parts = full_name.split()
    if len(parts) >= 2:
        # "LAST FIRST"
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}".upper())
        # "LAST, FIRST"
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}".upper())
        # "FIRST LAST" already in full_name
    return list(variants)


# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL DATA  (Collin CAD bulk DBF)
# ══════════════════════════════════════════════════════════════════════════════

@retry
def download_parcel_dbf() -> bytes | None:
    """
    Fetch the Collin CAD open-data portal page, find the parcel/property
    DBF download link (or a ZIP containing it) and return raw bytes.
    """
    log.info("Fetching CAD portal page: %s", CAD_PORTAL)
    resp = requests.get(CAD_PORTAL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Look for links that mention parcel, property, or DBF/ZIP
    candidates = []
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        text = a.get_text(" ", strip=True).upper()
        lower_href = href.lower()
        if any(kw in lower_href or kw in text for kw in
               ["parcel", "property", "apprais", ".dbf", ".zip"]):
            candidates.append(href)

    if not candidates:
        log.warning("No parcel download links found on CAD portal.")
        return None

    # Prefer DBF over ZIP; first hit wins
    for href in candidates:
        if not href.startswith("http"):
            href = requests.compat.urljoin(CAD_PORTAL, href)
        log.info("Trying parcel download: %s", href)
        try:
            r = requests.get(href, timeout=120, stream=True)
            r.raise_for_status()
            data = r.content
            log.info("Downloaded %d bytes from %s", len(data), href)
            return data
        except Exception as exc:
            log.warning("Failed to download %s: %s", href, exc)

    return None


def build_parcel_lookup(raw_bytes: bytes) -> dict[str, dict]:
    """
    Parse the DBF (or ZIP-of-DBF) bytes and return a dict keyed by
    upper-cased owner-name variants pointing to address dicts.
    """
    lookup: dict[str, dict] = {}

    def process_dbf_bytes(dbf_bytes: bytes) -> None:
        with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
            tmp.write(dbf_bytes)
            tmp_path = tmp.name
        try:
            tbl = DBF(tmp_path, ignore_missing_memofile=True)
            field_names_upper = [f.name.upper() for f in tbl.fields]

            def col(rec: dict, *candidates) -> str:
                for c in candidates:
                    v = rec.get(c) or rec.get(c.lower())
                    if v:
                        return safe_str(v)
                return ""

            for rec in tbl:
                # Normalize keys to upper
                rec_u = {k.upper(): v for k, v in rec.items()}

                owner = col(rec_u, "OWN1", "OWNER", "OWN_NAME", "OWNERNAME")
                if not owner:
                    continue

                parcel = {
                    "prop_address": col(rec_u, "SITEADDR", "SITE_ADDR",
                                        "SITE_ADDRESS", "PROPADDR", "ADDRESS"),
                    "prop_city":    col(rec_u, "SITECITY", "SITE_CITY",
                                        "PROP_CITY", "CITY"),
                    "prop_state":   col(rec_u, "SITESTATE", "SITE_STATE", "STATE") or "TX",
                    "prop_zip":     col(rec_u, "SITEZIP", "SITE_ZIP",
                                        "PROP_ZIP", "ZIP"),
                    "mail_address": col(rec_u, "MAILADR1", "ADDR_1",
                                        "MAIL_ADDR", "MAILADDR"),
                    "mail_city":    col(rec_u, "MAILCITY", "MAIL_CITY"),
                    "mail_state":   col(rec_u, "MAILSTATE", "MAIL_STATE") or "TX",
                    "mail_zip":     col(rec_u, "MAILZIP", "MAIL_ZIP"),
                }
                for variant in name_variants(owner):
                    if variant and variant not in lookup:
                        lookup[variant] = parcel

            log.info("Parcel lookup built: %d unique owner keys", len(lookup))
        finally:
            os.unlink(tmp_path)

    # Is it a ZIP?
    if raw_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
            if not dbf_names:
                log.warning("ZIP contained no .dbf files")
                return lookup
            for name in dbf_names:
                log.info("Processing DBF from ZIP: %s", name)
                process_dbf_bytes(zf.read(name))
    else:
        process_dbf_bytes(raw_bytes)

    return lookup


def lookup_parcel(owner: str, parcel_lookup: dict) -> dict:
    """Find parcel address data for an owner name."""
    for variant in name_variants(owner):
        if variant in parcel_lookup:
            return parcel_lookup[variant]
    return {}


# ══════════════════════════════════════════════════════════════════════════════
#  CLERK PORTAL  (Playwright async)
# ══════════════════════════════════════════════════════════════════════════════

async def clerk_search_doc_type(
    context: BrowserContext,
    doc_type_code: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """
    Search the Collin County clerk portal for a single document type
    over the given date range. Returns a list of raw record dicts.
    """
    page: Page = await context.new_page()
    results: list[dict] = []

    try:
        log.info("Searching clerk portal for %s (%s → %s)",
                 doc_type_code, date_from, date_to)

        # ── Navigate to search page ──────────────────────────────────────────
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                await page.goto(f"{CLERK_BASE}/results", timeout=30_000,
                                wait_until="domcontentloaded")
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS:
                    raise
                log.warning("Navigation attempt %d failed: %s", attempt, exc)
                await asyncio.sleep(RETRY_DELAY)

        # ── Fill search form ─────────────────────────────────────────────────
        # The portal uses a React/SPA with hidden inputs; we drive it via UI.
        # Try to locate the Advanced Search panel.
        try:
            # Accept cookies / dismiss modal if present
            dismiss = page.locator("button:has-text('Accept'), button:has-text('Close'), "
                                   "button:has-text('Dismiss')")
            if await dismiss.count() > 0:
                await dismiss.first.click(timeout=3000)
        except Exception:
            pass

        # Click "Advanced Search" or "Search" tab
        for label in ["Advanced Search", "Advanced", "Search"]:
            btn = page.locator(f"text={label}").first
            if await btn.count() > 0:
                await btn.click(timeout=5000)
                await asyncio.sleep(0.5)
                break

        # Document type input
        for selector in [
            "input[placeholder*='Document Type' i]",
            "input[name*='docType' i]",
            "input[id*='docType' i]",
            "input[aria-label*='Document Type' i]",
        ]:
            el = page.locator(selector).first
            if await el.count() > 0:
                await el.fill(doc_type_code)
                await asyncio.sleep(0.3)
                # select dropdown suggestion
                sugg = page.locator(f"li:has-text('{doc_type_code}'), "
                                    f"div[role='option']:has-text('{doc_type_code}')").first
                if await sugg.count() > 0:
                    await sugg.click(timeout=3000)
                break

        # Date from
        for selector in [
            "input[placeholder*='Start Date' i]",
            "input[name*='startDate' i]",
            "input[id*='startDate' i]",
            "input[placeholder*='From' i]",
        ]:
            el = page.locator(selector).first
            if await el.count() > 0:
                await el.fill(date_from)
                break

        # Date to
        for selector in [
            "input[placeholder*='End Date' i]",
            "input[name*='endDate' i]",
            "input[id*='endDate' i]",
            "input[placeholder*='To' i]",
        ]:
            el = page.locator(selector).first
            if await el.count() > 0:
                await el.fill(date_to)
                break

        # Submit
        for selector in [
            "button[type='submit']",
            "button:has-text('Search')",
            "input[type='submit']",
        ]:
            btn = page.locator(selector).first
            if await btn.count() > 0:
                await btn.click(timeout=5000)
                break

        await asyncio.sleep(2)

        # ── Paginate through results ─────────────────────────────────────────
        page_num = 0
        while True:
            page_num += 1
            log.debug("Parsing results page %d for %s", page_num, doc_type_code)

            await page.wait_for_load_state("networkidle", timeout=15_000)
            html = await page.content()
            page_results = _parse_clerk_results_html(html, doc_type_code)
            results.extend(page_results)

            if not page_results:
                break

            # Try to click "Next" page button
            next_btn = page.locator(
                "button:has-text('Next'), a:has-text('Next'), "
                "[aria-label='Next page'], [title='Next']"
            ).first
            if await next_btn.count() == 0:
                break
            is_disabled = await next_btn.get_attribute("disabled")
            cls = await next_btn.get_attribute("class") or ""
            if is_disabled is not None or "disabled" in cls.lower():
                break
            await next_btn.click(timeout=5000)
            await asyncio.sleep(1.5)

    except Exception as exc:
        log.error("clerk_search_doc_type(%s) error: %s", doc_type_code, exc)
    finally:
        await page.close()

    log.info("  → %d records found for %s", len(results), doc_type_code)
    return results


def _parse_clerk_results_html(html: str, doc_type_code: str) -> list[dict]:
    """Extract result rows from the clerk portal HTML."""
    soup = BeautifulSoup(html, "lxml")
    records: list[dict] = []

    # The portal renders results in a table or a card list.
    # Try table first.
    table = soup.find("table")
    if table:
        headers = [th.get_text(" ", strip=True).lower()
                   for th in table.find_all("th")]
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells:
                continue
            row = dict(zip(headers, cells))
            # Grab href link if present
            link_tag = tr.find("a", href=True)
            href = ""
            if link_tag:
                href = link_tag["href"]
                if not href.startswith("http"):
                    href = f"{CLERK_BASE}{href}"
            records.append(_normalise_clerk_row(row, href, doc_type_code))
        return records

    # Fallback: card-based layout
    for card in soup.find_all(class_=re.compile(r"result|record|item|card", re.I)):
        row: dict[str, str] = {}
        link_tag = card.find("a", href=True)
        href = ""
        if link_tag:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = f"{CLERK_BASE}{href}"

        for label in card.find_all(class_=re.compile(r"label|key|field-name", re.I)):
            key = label.get_text(" ", strip=True).lower().rstrip(":")
            value_tag = label.find_next_sibling()
            val = value_tag.get_text(" ", strip=True) if value_tag else ""
            row[key] = val

        if row:
            records.append(_normalise_clerk_row(row, href, doc_type_code))

    return records


def _normalise_clerk_row(row: dict, href: str, doc_type_code: str) -> dict:
    """Map arbitrary clerk field names to our canonical schema."""
    def g(*keys) -> str:
        for k in keys:
            v = row.get(k, "") or row.get(k.lower(), "") or row.get(k.upper(), "")
            if v:
                return safe_str(v)
        return ""

    return {
        "doc_num":   g("document number", "doc number", "instrument", "doc #",
                       "doc_num", "instrument number"),
        "doc_type":  g("document type", "type", "doc type") or doc_type_code,
        "filed":     g("filed", "file date", "recorded", "date filed", "date",
                       "recording date"),
        "owner":     g("grantor", "owner", "grantors"),
        "grantee":   g("grantee", "grantees"),
        "legal":     g("legal", "legal description", "description"),
        "amount":    g("amount", "consideration", "debt", "lien amount"),
        "clerk_url": href,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SCORING
# ══════════════════════════════════════════════════════════════════════════════

def compute_flags(rec: dict, today: datetime) -> list[str]:
    flags: list[str] = list(DOC_TYPE_MAP.get(rec["cat"], ("", "", []))[2])

    # LLC / Corp owner check
    owner_up = rec.get("owner", "").upper()
    if any(kw in owner_up for kw in ["LLC", "INC", "CORP", "LTD", "L.L.C",
                                      "TRUST", "HOLDINGS", "PROPERTIES"]):
        flags.append("LLC / corp owner")

    # New this week
    try:
        filed_dt = datetime.strptime(rec.get("filed", ""), "%m/%d/%Y")
        if (today - filed_dt).days <= 7:
            flags.append("New this week")
    except ValueError:
        pass

    # De-duplicate, preserve order
    seen: set[str] = set()
    unique: list[str] = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            unique.append(f)
    return unique


def compute_score(rec: dict, flags: list[str]) -> int:
    score = 30  # base

    # +10 per flag (capped at 4 flags = 40)
    score += min(len(flags), 4) * 10

    # +20 if both LP and foreclosure present
    flag_set = set(flags)
    if "Lis pendens" in flag_set and "Pre-foreclosure" in flag_set:
        score += 20

    # Amount bonuses
    amount = rec.get("amount_raw")
    if amount:
        try:
            amt = float(amount)
            if amt > 100_000:
                score += 15
            elif amt > 50_000:
                score += 10
        except (ValueError, TypeError):
            pass

    # Has address
    if rec.get("prop_address"):
        score += 5

    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

async def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    """Launch Playwright, scrape all doc types, return raw clerk records."""
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )

        for code in DOC_TYPE_MAP:
            try:
                recs = await clerk_search_doc_type(
                    context, code, date_from, date_to
                )
                for r in recs:
                    r["cat"] = code
                    r["cat_label"] = DOC_TYPE_MAP[code][1]
                all_records.extend(recs)
            except Exception as exc:
                log.error("Failed scraping %s: %s", code, exc)

        await browser.close()

    return all_records


def assemble_records(
    raw_records: list[dict],
    parcel_lookup: dict,
    today: datetime,
) -> list[dict]:
    assembled: list[dict] = []

    for raw in raw_records:
        try:
            owner = safe_str(raw.get("owner", ""))
            parcel = lookup_parcel(owner, parcel_lookup) if owner else {}

            amount_str = safe_str(raw.get("amount", ""))
            amount_raw = parse_amount(amount_str)

            rec: dict = {
                "doc_num":      safe_str(raw.get("doc_num", "")),
                "doc_type":     safe_str(raw.get("doc_type", "")),
                "filed":        safe_str(raw.get("filed", "")),
                "cat":          safe_str(raw.get("cat", "")),
                "cat_label":    safe_str(raw.get("cat_label", "")),
                "owner":        owner,
                "grantee":      safe_str(raw.get("grantee", "")),
                "amount":       amount_str,
                "amount_raw":   amount_raw,
                "legal":        safe_str(raw.get("legal", "")),
                "prop_address": parcel.get("prop_address", ""),
                "prop_city":    parcel.get("prop_city", ""),
                "prop_state":   parcel.get("prop_state", "TX"),
                "prop_zip":     parcel.get("prop_zip", ""),
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city", ""),
                "mail_state":   parcel.get("mail_state", "TX"),
                "mail_zip":     parcel.get("mail_zip", ""),
                "clerk_url":    safe_str(raw.get("clerk_url", "")),
            }

            flags = compute_flags(rec, today)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags)

            # Remove internal helper field before saving
            del rec["amount_raw"]

            assembled.append(rec)
        except Exception as exc:
            log.warning("Skipping bad record: %s — %s", raw, exc)

    # Sort by score descending
    assembled.sort(key=lambda r: r["score"], reverse=True)
    return assembled


def save_output(records: list[dict], date_from: str, date_to: str) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    with_address = sum(1 for r in records if r.get("prop_address"))

    payload = {
        "fetched_at":  fetched_at,
        "source":      "Collin County Clerk / Collin CAD",
        "date_range":  {"from": date_from, "to": date_to},
        "total":       len(records),
        "with_address": with_address,
        "records":     records,
    }

    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved %d records → %s", len(records), path)


def export_ghl_csv(records: list[dict]) -> None:
    """Export GoHighLevel-compatible CSV."""
    out_path = DATA_DIR / "ghl_export.csv"
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str) -> tuple[str, str]:
        parts = full.strip().split()
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return " ".join(parts[:-1]), parts[-1]

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for r in records:
            first, last = split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", "TX"),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", "TX"),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat", ""),
                "Document Type":         r.get("cat_label", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":               "Collin County Clerk",
                "Public Records URL":    r.get("clerk_url", ""),
            })

    log.info("GHL CSV exported → %s", out_path)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    today = datetime.now()
    start = today - timedelta(days=LOOKBACK_DAYS)
    date_from = start.strftime("%-m/%-d/%Y") if sys.platform != "win32" \
        else start.strftime("%#m/%#d/%Y")
    date_to = today.strftime("%-m/%-d/%Y") if sys.platform != "win32" \
        else today.strftime("%#m/%#d/%Y")

    log.info("═" * 60)
    log.info("Collin County Motivated Seller Scraper")
    log.info("Date range: %s → %s", date_from, date_to)
    log.info("═" * 60)

    # ── Step 1: Build parcel lookup ──────────────────────────────────────────
    parcel_lookup: dict = {}
    try:
        raw_bytes = download_parcel_dbf()
        if raw_bytes:
            parcel_lookup = build_parcel_lookup(raw_bytes)
        else:
            log.warning("No parcel data downloaded; addresses will be empty.")
    except Exception as exc:
        log.error("Parcel download failed: %s — continuing without addresses.", exc)

    # ── Step 2: Scrape clerk portal ──────────────────────────────────────────
    raw_records = await run_clerk_scrape(date_from, date_to)
    log.info("Total raw records from clerk: %d", len(raw_records))

    # ── Step 3: Assemble final records ───────────────────────────────────────
    records = assemble_records(raw_records, parcel_lookup, today)
    log.info("Final assembled records: %d", len(records))

    # ── Step 4: Save outputs ─────────────────────────────────────────────────
    save_output(records, date_from, date_to)
    export_ghl_csv(records)

    log.info("Done. %d records saved (%d with address).",
             len(records), sum(1 for r in records if r.get("prop_address")))


if __name__ == "__main__":
    asyncio.run(main())
