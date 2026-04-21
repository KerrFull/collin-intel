bash

cat > /mnt/user-data/outputs/collin-scraper/scraper/fetch.py << 'PYEOF'
#!/usr/bin/env python3
"""
Collin County, Texas — Motivated Seller Lead Scraper
Clerk portal : https://collin.tx.publicsearch.us/
Parcel data  : Texas Open Data Portal (Socrata) — Collin CAD Owner Prop List
               https://data.texas.gov/resource/ahis-pci3.json
               Fallback: Collin CAD Appraisal Data Preliminary
               https://data.texas.gov/resource/nne4-8riu.json
Look-back    : last 7 days
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
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
LOOKBACK_DAYS = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 5
DEBUG          = True

# Texas Open Data Portal — Collin CAD datasets (Socrata)
# Primary:  Owner Prop List  (owner + mailing address)
# Fallback: Appraisal Data Preliminary (full appraisal record)
SOCRATA_OWNER  = "https://data.texas.gov/resource/ahis-pci3.json"
SOCRATA_APPR   = "https://data.texas.gov/resource/nne4-8riu.json"
SOCRATA_LIMIT  = 1   # we only need 1 match per owner lookup

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
DEBUG_DIR     = ROOT / "debug"
for d in [DASHBOARD_DIR, DATA_DIR, DEBUG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

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

async def screenshot(page: Page, name: str) -> None:
    if not DEBUG: return
    try:
        await page.screenshot(path=str(DEBUG_DIR / f"{name}.png"), full_page=True)
        log.info("  📸 %s.png", name)
    except Exception as exc:
        log.warning("  Screenshot failed: %s", exc)

async def save_html(page: Page, name: str) -> None:
    if not DEBUG: return
    try:
        (DEBUG_DIR / f"{name}.html").write_text(await page.content(), encoding="utf-8")
        log.info("  📄 %s.html", name)
    except Exception as exc:
        log.warning("  HTML save failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL LOOKUP — Texas Open Data Portal (Socrata)
#
#  We do NOT download a bulk file. Instead we query per-owner on demand.
#  The Socrata API is free, no auth required, rate limit ~1000 req/hour.
#  We cache results in a dict to avoid duplicate calls for same owner.
#
#  Datasets used (in order of preference):
#    ahis-pci3  — "Collin CAD Owner Prop List"  (owner + site + mail)
#    nne4-8riu  — "Collin CAD Appraisal Data - Preliminary" (fallback)
#
#  Column names discovered from Texas Open Data Portal documentation and
#  TrueAutomation/Socrata standard CCAD layout.
# ══════════════════════════════════════════════════════════════════════════════

_parcel_cache: dict[str, dict] = {}

# Known possible column names for each field across CCAD Socrata datasets.
# We try all of them and take the first non-empty value.
_OWNER_COLS   = ["owner_name", "ownername", "owner", "own1", "own_name"]
_SADDR_COLS   = ["situs_num", "site_num", "situs_street_num"]
_SSTREET_COLS = ["situs_street", "site_street", "situs_str", "street_name"]
_SCITY_COLS   = ["situs_city", "site_city", "prop_city", "city_name"]
_SSTATE_COLS  = ["situs_state", "site_state", "state"]
_SZIP_COLS    = ["situs_zip", "site_zip", "zip_code", "zip"]
_MADDR_COLS   = ["mail_addr", "mailing_address", "mail_address", "addr_line1",
                 "mail_addr1", "mailing_addr"]
_MCITY_COLS   = ["mail_city", "mailing_city"]
_MSTATE_COLS  = ["mail_state", "mailing_state"]
_MZIP_COLS    = ["mail_zip", "mailing_zip", "mail_zipcode"]


def _first(row: dict, *cols) -> str:
    for c in cols:
        v = row.get(c, "")
        if v:
            return safe_str(v)
    return ""


def _row_to_parcel(row: dict) -> dict:
    """Convert a Socrata row to our parcel dict, trying all known column names."""
    # Build site address from num + street, or from a combined field
    snum    = _first(row, *_SADDR_COLS)
    sstreet = _first(row, *_SSTREET_COLS)
    # Some datasets have a combined situs_address field
    saddr_combined = _first(row, "situs_address", "site_address",
                             "prop_address", "situs_addr", "site_addr",
                             "location_address", "address")
    if saddr_combined:
        prop_address = saddr_combined
    elif snum and sstreet:
        prop_address = f"{snum} {sstreet}"
    else:
        prop_address = snum or sstreet

    return {
        "prop_address": prop_address,
        "prop_city":    _first(row, *_SCITY_COLS),
        "prop_state":   _first(row, *_SSTATE_COLS) or "TX",
        "prop_zip":     _first(row, *_SZIP_COLS),
        "mail_address": _first(row, *_MADDR_COLS),
        "mail_city":    _first(row, *_MCITY_COLS),
        "mail_state":   _first(row, *_MSTATE_COLS) or "TX",
        "mail_zip":     _first(row, *_MZIP_COLS),
    }


def _socrata_query(endpoint: str, owner_variant: str) -> list[dict]:
    """
    Query a Socrata endpoint for a single owner name variant.
    Uses $where= upper(owner_col) = 'NAME' across all known owner column names.
    """
    # Escape single quotes in name
    safe_name = owner_variant.replace("'", "''")
    results = []
    for col in _OWNER_COLS:
        try:
            resp = requests.get(
                endpoint,
                params={
                    "$where": f"upper({col}) = '{safe_name}'",
                    "$limit": SOCRATA_LIMIT,
                },
                timeout=8,
                headers={"Accept": "application/json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    results = data
                    break
        except Exception:
            pass
    return results


def lookup_parcel(owner: str) -> dict:
    """
    Look up a property address for an owner name.
    Checks cache first, then queries Socrata, tries both datasets.
    """
    if not owner:
        return {}

    cache_key = owner.strip().upper()
    if cache_key in _parcel_cache:
        return _parcel_cache[cache_key]

    for variant in name_variants(owner):
        # Try primary dataset first (Owner Prop List)
        for endpoint in [SOCRATA_OWNER, SOCRATA_APPR]:
            rows = _socrata_query(endpoint, variant)
            if rows:
                parcel = _row_to_parcel(rows[0])
                # Only cache and return if we actually got an address
                if parcel.get("prop_address") or parcel.get("mail_address"):
                    _parcel_cache[cache_key] = parcel
                    return parcel

    # Cache empty result to avoid re-querying
    _parcel_cache[cache_key] = {}
    return {}


# ══════════════════════════════════════════════════════════════════════════════
#  CLERK PORTAL — Neumo SPA  (Playwright)
# ══════════════════════════════════════════════════════════════════════════════

def build_search_url(doc_type: str, date_from: str, date_to: str) -> str:
    from urllib.parse import quote
    return (
        f"{CLERK_BASE}/results"
        f"?searchType=quickSearch&department=RP&searchOcrText=false"
        f"&searchTerm={doc_type}"
        f"&recordedDateRange=custom"
        f"&recordedDateFrom={quote(date_from, safe='')}"
        f"&recordedDateTo={quote(date_to, safe='')}"
    )


def _extract_from_api(body: Any, doc_type: str) -> list[dict]:
    hits: list = []
    if isinstance(body, dict):
        hits = (body.get("hits") or body.get("results") or
                body.get("records") or body.get("data") or [])
        if isinstance(hits, dict):
            hits = hits.get("hits") or hits.get("items") or []
    elif isinstance(body, list):
        hits = body

    records = []
    for hit in hits:
        if not isinstance(hit, dict): continue
        src = hit.get("_source") or hit
        def g(*keys) -> str:
            for k in keys:
                v = src.get(k)
                if v: return safe_str(v)
            return ""
        doc_num = g("instrumentNumber","docNumber","instrument_number","doc_number","InstrumentNumber")
        filed   = g("recordedDate","filedDate","recorded_date","file_date","RecordedDate")
        owner   = g("grantor","grantors","owner","Grantor")
        grantee = g("grantee","grantees","Grantee")
        legal   = g("legalDescription","legal_description","legal","Legal")
        amount  = g("consideration","amount","Amount","Consideration")
        dtype   = g("documentType","doc_type","DocumentType") or doc_type
        doc_id  = hit.get("id") or hit.get("_id") or hit.get("documentId") or ""
        clerk_url = f"{CLERK_BASE}/doc/{doc_id}" if doc_id else ""
        if doc_num or owner:
            records.append({
                "doc_num": doc_num, "doc_type": dtype,
                "filed": _normalise_date(filed), "owner": owner,
                "grantee": grantee, "legal": legal,
                "amount": amount, "clerk_url": clerk_url,
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
            v = row.get(k,"") or row.get(k.lower(),"") or row.get(k.upper(),"")
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
            link = tr.find("a", href=True)
            href = _abs(link["href"]) if link else ""
            records.append(_row_to_record(dict(zip(headers, cells)), href, doc_type))
        if records: return records

    # Card layout
    for sel in [
        "[data-testid='result-item']", "[class*='record-card']",
        "[class*='result-item']", "[class*='result-card']",
        "[class*='search-result']", "li[class*='result']",
        "div[class*='ResultItem']", "div[class*='RecordItem']",
        "div[class*='Hit']", "article",
    ]:
        cards = soup.select(sel)
        if cards:
            for card in cards:
                link = card.find("a", href=True)
                href = _abs(link["href"]) if link else ""
                row  = _text_to_row(card.get_text(" ", strip=True))
                rec  = _row_to_record(row, href, doc_type)
                if not rec["doc_num"] and href:
                    m = re.search(r"/doc/([^/?#]+)", href)
                    if m: rec["doc_num"] = m.group(1)
                if rec["doc_num"] or rec["owner"]:
                    records.append(rec)
            if records: return records

    return records


async def _click_next(page: Page) -> bool:
    btn = page.locator(
        "button:has-text('Next'), [aria-label='Next page'], "
        "[data-testid='next-page'], button[class*='next' i], a:has-text('Next')"
    ).first
    if await btn.count() == 0: return False
    disabled = await btn.get_attribute("disabled")
    cls = (await btn.get_attribute("class") or "").lower()
    if disabled is not None or "disabled" in cls: return False
    await btn.click()
    await asyncio.sleep(2.5)
    return True


async def _try_advanced_search(
    context: BrowserContext, doc_type: str,
    date_from: str, date_to: str,
) -> list[dict]:
    records: list[dict] = []
    page: Page = await context.new_page()
    api_responses: list[dict] = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if "json" in ct:
            try: api_responses.append(await response.json())
            except Exception: pass

    page.on("response", capture)
    try:
        await page.goto(f"{CLERK_BASE}/search/advanced", timeout=45_000, wait_until="networkidle")
        await asyncio.sleep(3)

        for sel in ["input[placeholder*='Document Type' i]", "input[name*='docType' i]",
                    "input[id*='docType' i]", "[data-testid*='docType' i]"]:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click(); await el.fill(doc_type); await asyncio.sleep(0.5)
                drop = page.locator(f"[role='option']:has-text('{doc_type}'), li:has-text('{doc_type}')").first
                if await drop.count() > 0: await drop.click()
                break

        for sel in ["input[placeholder*='Start' i]", "input[name*='start' i]",
                    "input[id*='start' i]", "input[placeholder*='From' i]"]:
            el = page.locator(sel).first
            if await el.count() > 0: await el.fill(date_from); break

        for sel in ["input[placeholder*='End' i]", "input[name*='end' i]",
                    "input[id*='end' i]", "input[placeholder*='To' i]"]:
            el = page.locator(sel).first
            if await el.count() > 0: await el.fill(date_to); break

        for sel in ["button[type='submit']", "button:has-text('Search')", "input[type='submit']"]:
            btn = page.locator(sel).first
            if await btn.count() > 0: await btn.click(); break

        await asyncio.sleep(4)
        for body in api_responses:
            records.extend(_extract_from_api(body, doc_type))
        if not records:
            records = await _parse_neumo_html(page, doc_type)
    except Exception as exc:
        log.warning("  Advanced search failed for %s: %s", doc_type, exc)
    finally:
        await page.close()
    return records


async def scrape_doc_type(
    context: BrowserContext, doc_type: str,
    date_from: str, date_to: str,
) -> list[dict]:
    page: Page = await context.new_page()
    records: list[dict] = []
    api_responses: list[dict] = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if "json" in ct and any(x in response.url for x in ["/api/", "/search", "/result", "/record"]):
            try: api_responses.append(await response.json())
            except Exception: pass

    page.on("response", capture)

    try:
        url = build_search_url(doc_type, date_from, date_to)
        log.info("  %s", url)

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                await page.goto(url, timeout=60_000, wait_until="networkidle")
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS: raise
                log.warning("    nav attempt %d: %s", attempt, exc)
                await asyncio.sleep(RETRY_DELAY)

        await asyncio.sleep(4)

        if doc_type == "LP":
            await screenshot(page, "search_LP")
            await save_html(page, "search_LP")

        title = await page.title()
        log.info("  title: %s | api responses: %d", title, len(api_responses))

        # Strategy 1: intercepted API JSON
        if api_responses:
            for body in api_responses:
                records.extend(_extract_from_api(body, doc_type))
            if records:
                if DEBUG and api_responses:
                    (DEBUG_DIR / f"api_{doc_type}.json").write_text(
                        json.dumps(api_responses[0], indent=2, default=str)[:50000])
                for _ in range(20):
                    api_responses.clear()
                    if not await _click_next(page): break
                    for body in api_responses:
                        records.extend(_extract_from_api(body, doc_type))

        # Strategy 2: parse HTML
        if not records:
            records = await _parse_neumo_html(page, doc_type)
            for _ in range(20):
                if not await _click_next(page): break
                new = await _parse_neumo_html(page, doc_type)
                if not new: break
                records.extend(new)

        # Strategy 3: advanced search form
        if not records:
            records = await _try_advanced_search(context, doc_type, date_from, date_to)

        log.info("  → %d records for %s", len(records), doc_type)

    except Exception as exc:
        log.error("scrape_doc_type(%s): %s", doc_type, exc)
        if DEBUG: await screenshot(page, f"error_{doc_type}")
    finally:
        await page.close()

    return records


async def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled",
                  "--disable-infobars", "--window-size=1280,900"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
        """)

        # Warm-up
        warmup = await context.new_page()
        try:
            await warmup.goto(CLERK_BASE, timeout=30_000, wait_until="networkidle")
            await asyncio.sleep(3)
            await screenshot(warmup, "homepage")
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
    owner_up = rec.get("owner", "").upper()
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
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
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

def assemble_records(raw_records: list[dict], today: datetime) -> list[dict]:
    assembled: list[dict] = []
    seen: set[str] = set()
    total = len(raw_records)

    for i, raw in enumerate(raw_records, 1):
        try:
            doc_num = safe_str(raw.get("doc_num", ""))
            if doc_num and doc_num in seen: continue
            if doc_num: seen.add(doc_num)

            owner      = safe_str(raw.get("owner", ""))
            amount_str = safe_str(raw.get("amount", ""))
            amount_raw = parse_amount(amount_str)

            # Live parcel lookup — show progress every 10 records
            if i % 10 == 0:
                log.info("  Parcel lookup progress: %d/%d (cache size: %d)",
                         i, total, len(_parcel_cache))
            parcel = lookup_parcel(owner)

            rec: dict = {
                "doc_num":      doc_num,
                "doc_type":     safe_str(raw.get("doc_type", "")),
                "filed":        safe_str(raw.get("filed", "")),
                "cat":          safe_str(raw.get("cat", "")),
                "cat_label":    safe_str(raw.get("cat_label", "")),
                "owner":        owner,
                "grantee":      safe_str(raw.get("grantee", "")),
                "amount":       amount_str,
                "_amount_raw":  amount_raw,
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
            del rec["_amount_raw"]
            assembled.append(rec)
        except Exception as exc:
            log.warning("Skipping bad record: %s", exc)

    assembled.sort(key=lambda r: r["score"], reverse=True)
    with_addr = sum(1 for r in assembled if r.get("prop_address"))
    log.info("Assembled %d unique records, %d with address", len(assembled), with_addr)
    return assembled


def save_output(records: list[dict], date_from: str, date_to: str) -> None:
    payload = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Collin County Clerk / Collin CAD (Texas Open Data)",
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
        if len(parts) == 1: return parts[0], ""
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
    log.info("Parcel source: Texas Open Data Portal (live lookup)")
    log.info("═" * 60)

    # Scrape clerk portal
    raw_records = await run_clerk_scrape(date_from, date_to)
    log.info("Raw records from clerk: %d", len(raw_records))

    # Assemble (includes live parcel lookups)
    records = assemble_records(raw_records, today)

    # Save outputs
    save_output(records, date_from, date_to)
    export_ghl_csv(records)

    log.info("Complete. %d records, %d with address.",
             len(records), sum(1 for r in records if r.get("prop_address")))


if __name__ == "__main__":
    asyncio.run(main())
PYEOF
echo "Done — $(wc -l < /mnt/user-data/outputs/collin-scraper/scraper/fetch.py) lines"
Output

Done — 779 lines
