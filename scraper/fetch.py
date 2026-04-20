#!/usr/bin/env python3
"""
Collin County, Texas — Motivated Seller Lead Scraper
Clerk portal : https://collin.tx.publicsearch.us/   (Neumo SPA)
Parcel data  : https://collincad.org/open-data-portal/
Look-back    : last 7 days
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

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("collin_scraper")

# ─── Constants ────────────────────────────────────────────────────────────────
CLERK_BASE    = "https://collin.tx.publicsearch.us"
CAD_PORTAL    = "https://collincad.org/open-data-portal/"
LOOKBACK_DAYS = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 5

DOC_TYPE_MAP: dict[str, tuple[str, str, list[str]]] = {
    "LP":       ("LP",       "Lis Pendens",             ["Lis pendens", "Pre-foreclosure"]),
    "RELLP":    ("RELLP",    "Release Lis Pendens",     ["Lis pendens"]),
    "NOFC":     ("NOFC",     "Notice of Foreclosure",   ["Pre-foreclosure"]),
    "TAXDEED":  ("TAXDEED",  "Tax Deed",                ["Tax lien"]),
    "JUD":      ("JUD",      "Judgment",                ["Judgment lien"]),
    "CCJ":      ("CCJ",      "Certified Judgment",      ["Judgment lien"]),
    "DRJUD":    ("DRJUD",    "Domestic Judgment",       ["Judgment lien"]),
    "LNCORPTX": ("LNCORPTX", "Corp Tax Lien",           ["Tax lien"]),
    "LNIRS":    ("LNIRS",    "IRS Lien",                ["Tax lien"]),
    "LNFED":    ("LNFED",    "Federal Lien",            ["Tax lien"]),
    "LN":       ("LN",       "Lien",                    ["Mechanic lien"]),
    "LNMECH":   ("LNMECH",   "Mechanic Lien",           ["Mechanic lien"]),
    "LNHOA":    ("LNHOA",    "HOA Lien",                ["Mechanic lien"]),
    "MEDLN":    ("MEDLN",    "Medicaid Lien",           ["Judgment lien"]),
    "PRO":      ("PRO",      "Probate Document",        ["Probate / estate"]),
    "NOC":      ("NOC",      "Notice of Commencement",  []),
}

ROOT          = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def with_retry(fn, *args, **kwargs):
    last_exc = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            log.warning("Attempt %d/%d failed: %s", attempt, RETRY_ATTEMPTS, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    raise last_exc


def safe_str(val: Any) -> str:
    return str(val).strip() if val is not None else ""


def parse_amount(raw: str) -> float | None:
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def name_variants(full_name: str) -> list[str]:
    full_name = full_name.strip().upper()
    variants = {full_name}
    parts = full_name.split()
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    return [v for v in variants if v]


def _abs(href: str) -> str:
    if not href:
        return ""
    return href if href.startswith("http") else f"{CLERK_BASE}{href}"


def _normalise_date(raw: str) -> str:
    if not raw:
        return ""
    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', raw):
        return raw
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', raw)
    if m:
        return f"{m.group(2)}/{m.group(3)}/{m.group(1)}"
    return raw


# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL DATA
# ══════════════════════════════════════════════════════════════════════════════

def download_parcel_dbf() -> bytes | None:
    log.info("Fetching CAD portal: %s", CAD_PORTAL)
    resp = requests.get(CAD_PORTAL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        text = a.get_text(" ", strip=True).upper()
        lo   = href.lower()
        if any(kw in lo or kw in text
               for kw in ["parcel", "property", "apprais", ".dbf", ".zip"]):
            candidates.append(href)

    if not candidates:
        log.warning("No parcel download links found on CAD portal.")
        return None

    for href in candidates:
        if not href.startswith("http"):
            href = requests.compat.urljoin(CAD_PORTAL, href)
        log.info("Trying parcel download: %s", href)
        try:
            r = requests.get(href, timeout=120, stream=True)
            r.raise_for_status()
            data = r.content
            log.info("Downloaded %d bytes", len(data))
            return data
        except Exception as exc:
            log.warning("Failed %s: %s", href, exc)
    return None


def build_parcel_lookup(raw_bytes: bytes) -> dict[str, dict]:
    lookup: dict[str, dict] = {}

    def process(dbf_bytes: bytes) -> None:
        with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
            tmp.write(dbf_bytes)
            tmp_path = tmp.name
        try:
            tbl = DBF(tmp_path, ignore_missing_memofile=True)

            def col(rec: dict, *keys) -> str:
                for k in keys:
                    v = rec.get(k) or rec.get(k.lower())
                    if v:
                        return safe_str(v)
                return ""

            for rec in tbl:
                ru = {k.upper(): v for k, v in rec.items()}
                owner = col(ru, "OWN1", "OWNER", "OWN_NAME", "OWNERNAME")
                if not owner:
                    continue
                parcel = {
                    "prop_address": col(ru, "SITEADDR","SITE_ADDR","SITE_ADDRESS","PROPADDR","ADDRESS"),
                    "prop_city":    col(ru, "SITECITY","SITE_CITY","PROP_CITY","CITY"),
                    "prop_state":   col(ru, "SITESTATE","SITE_STATE","STATE") or "TX",
                    "prop_zip":     col(ru, "SITEZIP","SITE_ZIP","PROP_ZIP","ZIP"),
                    "mail_address": col(ru, "MAILADR1","ADDR_1","MAIL_ADDR","MAILADDR"),
                    "mail_city":    col(ru, "MAILCITY","MAIL_CITY"),
                    "mail_state":   col(ru, "MAILSTATE","MAIL_STATE") or "TX",
                    "mail_zip":     col(ru, "MAILZIP","MAIL_ZIP"),
                }
                for variant in name_variants(owner):
                    if variant not in lookup:
                        lookup[variant] = parcel
            log.info("Parcel lookup: %d owner keys", len(lookup))
        finally:
            os.unlink(tmp_path)

    if raw_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            for name in [n for n in zf.namelist() if n.lower().endswith(".dbf")]:
                process(zf.read(name))
    else:
        process(raw_bytes)
    return lookup


def lookup_parcel(owner: str, parcel_lookup: dict) -> dict:
    for variant in name_variants(owner):
        if variant in parcel_lookup:
            return parcel_lookup[variant]
    return {}


# ══════════════════════════════════════════════════════════════════════════════
#  CLERK PORTAL — Neumo SPA
#
#  URL pattern (observed from portal source):
#  /results?searchType=quickSearch&department=RP&searchOcrText=false
#           &searchTerm=<DOCTYPE>
#           &recordedDateRange=custom
#           &recordedDateFrom=MM%2FDD%2FYYYY&recordedDateTo=MM%2FDD%2FYYYY
# ══════════════════════════════════════════════════════════════════════════════

def build_search_url(doc_type: str, date_from: str, date_to: str) -> str:
    from urllib.parse import quote
    df = quote(date_from, safe="")
    dt = quote(date_to,   safe="")
    return (
        f"{CLERK_BASE}/results"
        f"?searchType=quickSearch&department=RP&searchOcrText=false"
        f"&searchTerm={doc_type}"
        f"&recordedDateRange=custom"
        f"&recordedDateFrom={df}&recordedDateTo={dt}"
    )


def _extract_from_api(body: Any, doc_type: str) -> list[dict]:
    records: list[dict] = []
    hits: list = []

    if isinstance(body, dict):
        hits = (body.get("hits") or body.get("results") or
                body.get("records") or body.get("data") or [])
        if isinstance(hits, dict):
            hits = hits.get("hits") or hits.get("items") or []
    elif isinstance(body, list):
        hits = body

    for hit in hits:
        if not isinstance(hit, dict):
            continue
        src = hit.get("_source") or hit

        def g(*keys) -> str:
            for k in keys:
                v = src.get(k)
                if v:
                    return safe_str(v)
            return ""

        doc_num  = g("instrumentNumber","docNumber","instrument_number","doc_number","InstrumentNumber")
        filed    = g("recordedDate","filedDate","recorded_date","file_date","RecordedDate")
        owner    = g("grantor","grantors","owner","Grantor")
        grantee  = g("grantee","grantees","Grantee")
        legal    = g("legalDescription","legal_description","legal","Legal")
        amount   = g("consideration","amount","Amount","Consideration")
        dtype    = g("documentType","doc_type","DocumentType") or doc_type
        doc_id   = hit.get("id") or hit.get("_id") or hit.get("documentId") or ""
        clerk_url = f"{CLERK_BASE}/doc/{doc_id}" if doc_id else ""

        if doc_num or owner:
            records.append({
                "doc_num":   doc_num,
                "doc_type":  dtype,
                "filed":     _normalise_date(filed),
                "owner":     owner,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    amount,
                "clerk_url": clerk_url,
            })
    return records


def _text_to_row(text: str) -> dict[str, str]:
    row: dict[str, str] = {}
    m = re.search(r'(\d{4}-\d{5,}|\d{8,})', text)
    if m: row["document number"] = m.group(1)
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
    if m: row["filed"] = m.group(1)
    m = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
    if m: row["amount"] = m.group(0)
    m = re.search(r'Grantor[:\s]+([^\n|]+)', text, re.I)
    if m: row["grantor"] = m.group(1).strip()
    m = re.search(r'Grantee[:\s]+([^\n|]+)', text, re.I)
    if m: row["grantee"] = m.group(1).strip()
    return row


def _row_to_record(row: dict, href: str, doc_type: str) -> dict:
    def g(*keys) -> str:
        for k in keys:
            v = row.get(k, "") or row.get(k.lower(), "") or row.get(k.upper(), "")
            if v: return safe_str(v)
        return ""
    return {
        "doc_num":   g("document number","doc number","instrument","doc #","doc_num","instrumentnumber"),
        "doc_type":  g("document type","type","doc type","documenttype") or doc_type,
        "filed":     _normalise_date(g("filed","file date","recorded","date filed","date","recordeddate")),
        "owner":     g("grantor","owner","grantors"),
        "grantee":   g("grantee","grantees"),
        "legal":     g("legal","legal description","description","legaldescription"),
        "amount":    g("amount","consideration","debt","lien amount"),
        "clerk_url": href,
    }


async def _parse_neumo_html(page: Page, doc_type: str) -> list[dict]:
    records: list[dict] = []
    html = await page.content()
    soup = BeautifulSoup(html, "lxml")

    if soup.find(string=re.compile(r"no results|0 results|nothing found|no records", re.I)):
        return records

    # Table layout
    table = soup.find("table")
    if table:
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells: continue
            row  = dict(zip(headers, cells))
            link = tr.find("a", href=True)
            href = _abs(link["href"]) if link else ""
            records.append(_row_to_record(row, href, doc_type))
        if records:
            return records

    # Card layout
    card_selectors = [
        "[data-testid='result-item']", "[class*='record-card']",
        "[class*='result-item']",      "[class*='result-card']",
        "[class*='search-result']",    "li[class*='result']",
        "div[class*='ResultItem']",    "div[class*='RecordItem']",
    ]
    cards = []
    for sel in card_selectors:
        found = soup.select(sel)
        if found:
            cards = found
            break

    for card in cards:
        link = card.find("a", href=True)
        href = _abs(link["href"]) if link else ""
        text = card.get_text(" ", strip=True)
        row  = _text_to_row(text)
        rec  = _row_to_record(row, href, doc_type)
        if not rec["doc_num"] and href:
            m = re.search(r"/doc/([^/?#]+)", href)
            if m: rec["doc_num"] = m.group(1)
        if rec["doc_num"] or rec["owner"]:
            records.append(rec)

    return records


async def _click_next(page: Page) -> bool:
    """Click Next page button. Returns True if clicked, False if not found/disabled."""
    next_btn = page.locator(
        "button:has-text('Next'), [aria-label='Next page'], "
        "[data-testid='next-page'], button[class*='next' i]"
    ).first
    if await next_btn.count() == 0:
        return False
    disabled = await next_btn.get_attribute("disabled")
    cls = (await next_btn.get_attribute("class") or "").lower()
    if disabled is not None or "disabled" in cls:
        return False
    await next_btn.click()
    await asyncio.sleep(2)
    return True


async def scrape_doc_type(
    context: BrowserContext,
    doc_type: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    page: Page = await context.new_page()
    records: list[dict] = []
    api_responses: list[dict] = []

    async def handle_response(response):
        url = response.url
        if "/api/" in url and ("search" in url.lower() or "result" in url.lower()):
            try:
                body = await response.json()
                api_responses.append(body)
            except Exception:
                pass

    page.on("response", handle_response)

    try:
        url = build_search_url(doc_type, date_from, date_to)
        log.info("  Fetching: %s", url)

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                await page.goto(url, timeout=45_000, wait_until="networkidle")
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS: raise
                log.warning("    nav attempt %d: %s", attempt, exc)
                await asyncio.sleep(RETRY_DELAY)

        await asyncio.sleep(3)

        # Strategy 1: intercepted API JSON
        if api_responses:
            for body in api_responses:
                records.extend(_extract_from_api(body, doc_type))
            if records:
                for _ in range(20):
                    api_responses.clear()
                    if not await _click_next(page): break
                    for body in api_responses:
                        records.extend(_extract_from_api(body, doc_type))

        # Strategy 2: parse rendered HTML
        if not records:
            records = await _parse_neumo_html(page, doc_type)
            for _ in range(20):
                if not await _click_next(page): break
                new = await _parse_neumo_html(page, doc_type)
                if not new: break
                records.extend(new)

        log.info("  %d records for %s", len(records), doc_type)

    except Exception as exc:
        log.error("scrape_doc_type(%s): %s", doc_type, exc)
    finally:
        await page.close()

    return records


async def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )

        # Warm-up: establish session cookies
        warmup = await context.new_page()
        try:
            await warmup.goto(CLERK_BASE, timeout=30_000, wait_until="networkidle")
            await asyncio.sleep(2)
        except Exception as exc:
            log.warning("Warmup non-fatal: %s", exc)
        finally:
            await warmup.close()

        for code in DOC_TYPE_MAP:
            try:
                recs = await scrape_doc_type(context, code, date_from, date_to)
                for r in recs:
                    r["cat"]       = code
                    r["cat_label"] = DOC_TYPE_MAP[code][1]
                all_records.extend(recs)
            except Exception as exc:
                log.error("Failed %s: %s", code, exc)
            await asyncio.sleep(2)

        await browser.close()

    return all_records


# ══════════════════════════════════════════════════════════════════════════════
#  SCORING
# ══════════════════════════════════════════════════════════════════════════════

def compute_flags(rec: dict, today: datetime) -> list[str]:
    flags: list[str] = list(DOC_TYPE_MAP.get(rec["cat"], ("","",[])) [2])
    owner_up = rec.get("owner","").upper()
    if any(kw in owner_up for kw in
           ["LLC","INC","CORP","LTD","L.L.C","TRUST","HOLDINGS","PROPERTIES"]):
        flags.append("LLC / corp owner")
    try:
        filed_dt = datetime.strptime(rec.get("filed",""), "%m/%d/%Y")
        if (today - filed_dt).days <= 7:
            flags.append("New this week")
    except ValueError:
        pass
    seen: set[str] = set()
    return [f for f in flags if not (f in seen or seen.add(f))]


def compute_score(rec: dict, flags: list[str]) -> int:
    score = 30
    score += min(len(flags), 4) * 10
    flag_set = set(flags)
    if "Lis pendens" in flag_set and "Pre-foreclosure" in flag_set:
        score += 20
    amount_raw = rec.get("_amount_raw")
    if amount_raw:
        try:
            amt = float(amount_raw)
            score += 15 if amt > 100_000 else (10 if amt > 50_000 else 0)
        except (ValueError, TypeError):
            pass
    if rec.get("prop_address"):
        score += 5
    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
#  ASSEMBLE + SAVE
# ══════════════════════════════════════════════════════════════════════════════

def assemble_records(raw_records: list[dict], parcel_lookup: dict, today: datetime) -> list[dict]:
    assembled: list[dict] = []
    seen: set[str] = set()

    for raw in raw_records:
        try:
            doc_num = safe_str(raw.get("doc_num",""))
            if doc_num and doc_num in seen: continue
            if doc_num: seen.add(doc_num)

            owner  = safe_str(raw.get("owner",""))
            parcel = lookup_parcel(owner, parcel_lookup) if owner else {}
            amount_str = safe_str(raw.get("amount",""))
            amount_raw = parse_amount(amount_str)

            rec: dict = {
                "doc_num":      doc_num,
                "doc_type":     safe_str(raw.get("doc_type","")),
                "filed":        safe_str(raw.get("filed","")),
                "cat":          safe_str(raw.get("cat","")),
                "cat_label":    safe_str(raw.get("cat_label","")),
                "owner":        owner,
                "grantee":      safe_str(raw.get("grantee","")),
                "amount":       amount_str,
                "_amount_raw":  amount_raw,
                "legal":        safe_str(raw.get("legal","")),
                "prop_address": parcel.get("prop_address",""),
                "prop_city":    parcel.get("prop_city",""),
                "prop_state":   parcel.get("prop_state","TX"),
                "prop_zip":     parcel.get("prop_zip",""),
                "mail_address": parcel.get("mail_address",""),
                "mail_city":    parcel.get("mail_city",""),
                "mail_state":   parcel.get("mail_state","TX"),
                "mail_zip":     parcel.get("mail_zip",""),
                "clerk_url":    safe_str(raw.get("clerk_url","")),
            }
            flags = compute_flags(rec, today)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags)
            del rec["_amount_raw"]
            assembled.append(rec)
        except Exception as exc:
            log.warning("Skipping bad record: %s", exc)

    assembled.sort(key=lambda r: r["score"], reverse=True)
    log.info("Assembled %d unique records", len(assembled))
    return assembled


def save_output(records: list[dict], date_from: str, date_to: str) -> None:
    payload = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Collin County Clerk / Collin CAD",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info("Saved → %s", path)


def export_ghl_csv(records: list[dict]) -> None:
    out_path = DATA_DIR / "ghl_export.csv"
    cols = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    def split_name(full: str) -> tuple[str, str]:
        parts = full.strip().split()
        if not parts: return "", ""
        if len(parts)==1: return parts[0], ""
        return " ".join(parts[:-1]), parts[-1]

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for r in records:
            first, last = split_name(r.get("owner",""))
            writer.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address",""),
                "Mailing City": r.get("mail_city",""),
                "Mailing State": r.get("mail_state","TX"),
                "Mailing Zip": r.get("mail_zip",""),
                "Property Address": r.get("prop_address",""),
                "Property City": r.get("prop_city",""),
                "Property State": r.get("prop_state","TX"),
                "Property Zip": r.get("prop_zip",""),
                "Lead Type": r.get("cat",""),
                "Document Type": r.get("cat_label",""),
                "Date Filed": r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed": r.get("amount",""),
                "Seller Score": r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source": "Collin County Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s", out_path)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    today = datetime.now()
    start = today - timedelta(days=LOOKBACK_DAYS)
    fmt = "%-m/%-d/%Y" if sys.platform != "win32" else "%#m/%#d/%Y"
    date_from = start.strftime(fmt)
    date_to   = today.strftime(fmt)

    log.info("═" * 60)
    log.info("Collin County Motivated Seller Scraper")
    log.info("Range: %s → %s", date_from, date_to)
    log.info("═" * 60)

    parcel_lookup: dict = {}
    try:
        raw = with_retry(download_parcel_dbf)
        if raw:
            parcel_lookup = build_parcel_lookup(raw)
    except Exception as exc:
        log.error("Parcel data unavailable: %s", exc)

    raw_records = await run_clerk_scrape(date_from, date_to)
    log.info("Raw records from clerk: %d", len(raw_records))

    records = assemble_records(raw_records, parcel_lookup, today)
    save_output(records, date_from, date_to)
    export_ghl_csv(records)

    log.info("Complete. %d records, %d with address.",
             len(records), sum(1 for r in records if r.get("prop_address")))


if __name__ == "__main__":
    asyncio.run(main())

