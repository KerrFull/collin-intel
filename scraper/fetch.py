#!/usr/bin/env python3
"""
Collin County, Texas — Motivated Seller Lead Scraper
Clerk portal : https://collin.tx.publicsearch.us/
Parcel data  : Texas Open Data Socrata ahis-pci3
               Confirmed fields: ownername, owneraddrline1, owneraddrcity,
               owneraddrstate, owneraddrzip, situsconcat
Look-back    : last 7 days
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, BrowserContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("collin_scraper")

CLERK_BASE     = "https://collin.tx.publicsearch.us"
LOOKBACK_DAYS  = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 5
DEBUG          = True

SOCRATA_OWNER = "https://data.texas.gov/resource/ahis-pci3.json"
SOCRATA_APPR  = "https://data.texas.gov/resource/nne4-8riu.json"

ARCGIS_URL = (
    "https://gismaps.cityofallen.org/arcgis/rest/services/"
    "ReferenceData/Collin_County_Appraisal_District_Parcels/MapServer/1/query"
)

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
for _d in [DASHBOARD_DIR, DATA_DIR, DEBUG_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


# ==============================================================================
#  HELPERS
# ==============================================================================

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
    variants: list[str] = [full_name]
    parts = full_name.split()
    if len(parts) >= 2:
        flipped = f"{' '.join(parts[1:])} {parts[0]}"
        if flipped not in variants:
            variants.append(flipped)
        comma = f"{parts[0]}, {' '.join(parts[1:])}"
        if comma not in variants:
            variants.append(comma)
        if len(parts) >= 3:
            alt = f"{' '.join(parts[1:])} {parts[0]}"
            if alt not in variants:
                variants.append(alt)
    return variants

def _abs_url(href: str) -> str:
    if not href:
        return ""
    return href if href.startswith("http") else f"{CLERK_BASE}{href}"

def _normalise_date(raw: str) -> str:
    if not raw:
        return ""
    if re.match(r"\d{1,2}/\d{1,2}/\d{4}", raw):
        return raw
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(2)}/{m.group(3)}/{m.group(1)}"
    return raw

def _parse_situsconcat(situs: str) -> tuple[str, str, str, str]:
    situs = situs.strip()
    zip_m = re.search(r"\b(\d{5})(?:-\d{4})?\s*$", situs)
    prop_zip = zip_m.group(1) if zip_m else ""
    remainder = situs[:zip_m.start()].strip(" ,") if zip_m else situs
    st_m = re.search(r",?\s*([A-Z]{2})\s*$", remainder)
    prop_state = st_m.group(1) if st_m else "TX"
    remainder = remainder[:st_m.start()].strip(" ,") if st_m else remainder
    parts = remainder.rsplit(",", 1)
    if len(parts) == 2:
        prop_addr = parts[0].strip()
        prop_city = parts[1].strip()
    else:
        prop_addr = remainder.strip()
        prop_city = ""
    return prop_addr, prop_city, prop_state, prop_zip

async def screenshot(page: Page, name: str) -> None:
    if not DEBUG:
        return
    try:
        await page.screenshot(path=str(DEBUG_DIR / f"{name}.png"), full_page=True)
        log.info("  screenshot: %s.png", name)
    except Exception as exc:
        log.warning("  screenshot failed: %s", exc)

async def save_html(page: Page, name: str) -> None:
    if not DEBUG:
        return
    try:
        (DEBUG_DIR / f"{name}.html").write_text(
            await page.content(), encoding="utf-8")
    except Exception:
        pass


# ==============================================================================
#  PARCEL LOOKUP
# ==============================================================================

_parcel_cache: dict[str, dict] = {}

def _socrata_lookup(owner_variant: str) -> dict:
    safe_name = owner_variant.replace("'", "''")
    for endpoint in [SOCRATA_OWNER, SOCRATA_APPR]:
        try:
            resp = requests.get(
                endpoint,
                params={
                    "$where": f"ownername = '{safe_name}'",
                    "$limit": 1,
                },
                timeout=10,
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                continue
            rows = resp.json()
            if not (isinstance(rows, list) and rows):
                continue
            r = rows[0]
            situs      = safe_str(r.get("situsconcat", ""))
            mail_addr  = safe_str(r.get("owneraddrline1", ""))
            mail_city  = safe_str(r.get("owneraddrcity", ""))
            mail_state = safe_str(r.get("owneraddrstate", "")) or "TX"
            mail_zip   = safe_str(r.get("owneraddrzip", ""))
            if "-" in mail_zip:
                mail_zip = mail_zip.split("-")[0]
            prop_addr, prop_city, prop_state, prop_zip = _parse_situsconcat(situs)
            if prop_addr or mail_addr:
                return {
                    "prop_address": prop_addr,
                    "prop_city":    prop_city,
                    "prop_state":   prop_state or "TX",
                    "prop_zip":     prop_zip,
                    "mail_address": mail_addr,
                    "mail_city":    mail_city,
                    "mail_state":   mail_state,
                    "mail_zip":     mail_zip,
                }
        except Exception as exc:
            log.debug("Socrata error %r: %s", owner_variant[:40], exc)
    return {}

def _socrata_fuzzy(owner_variant: str) -> dict:
    parts = owner_variant.strip().split()
    if not parts:
        return {}
    search_word = next((p for p in parts if len(p) > 2), parts[0])
    safe_word = search_word.replace("'", "''")
    for endpoint in [SOCRATA_OWNER, SOCRATA_APPR]:
        try:
            resp = requests.get(
                endpoint,
                params={
                    "$where": f"ownername LIKE '%{safe_word}%'",
                    "$limit": 5,
                },
                timeout=10,
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                continue
            rows = resp.json()
            if not (isinstance(rows, list) and rows):
                continue
            for r in rows:
                rname = safe_str(r.get("ownername", "")).upper()
                sig_parts = [p for p in parts if len(p) > 2]
                if all(p in rname for p in sig_parts):
                    situs     = safe_str(r.get("situsconcat", ""))
                    mail_addr = safe_str(r.get("owneraddrline1", ""))
                    mail_city = safe_str(r.get("owneraddrcity", ""))
                    mail_state= safe_str(r.get("owneraddrstate", "")) or "TX"
                    mail_zip  = safe_str(r.get("owneraddrzip", ""))
                    if "-" in mail_zip:
                        mail_zip = mail_zip.split("-")[0]
                    prop_addr, prop_city, prop_state, prop_zip = \
                        _parse_situsconcat(situs)
                    if prop_addr or mail_addr:
                        return {
                            "prop_address": prop_addr,
                            "prop_city":    prop_city,
                            "prop_state":   prop_state or "TX",
                            "prop_zip":     prop_zip,
                            "mail_address": mail_addr,
                            "mail_city":    mail_city,
                            "mail_state":   mail_state,
                            "mail_zip":     mail_zip,
                        }
        except Exception as exc:
            log.debug("Socrata fuzzy error %r: %s", owner_variant[:40], exc)
    return {}

def _arcgis_lookup(owner_variant: str) -> dict:
    safe_name = owner_variant.replace("'", "''")
    try:
        resp = requests.get(
            ARCGIS_URL,
            params={
                "where": f"file_as_name LIKE '{safe_name}'",
                "outFields": (
                    "file_as_name,addr_line1,addr_city,addr_state,addr_zip,"
                    "situs_num,situs_street,situs_city"
                ),
                "returnGeometry": "false",
                "resultRecordCount": 1,
                "f": "json",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        if data.get("error"):
            return {}
        features = data.get("features", [])
        if not features:
            return {}
        attrs = features[0].get("attributes", {})
        snum  = safe_str(attrs.get("situs_num", ""))
        sstr  = safe_str(attrs.get("situs_street", ""))
        return {
            "prop_address": f"{snum} {sstr}".strip(),
            "prop_city":    safe_str(attrs.get("situs_city", "")),
            "prop_state":   "TX",
            "prop_zip":     "",
            "mail_address": safe_str(attrs.get("addr_line1", "")),
            "mail_city":    safe_str(attrs.get("addr_city", "")),
            "mail_state":   safe_str(attrs.get("addr_state", "")) or "TX",
            "mail_zip":     safe_str(attrs.get("addr_zip", "")),
        }
    except Exception as exc:
        log.debug("ArcGIS error %r: %s", owner_variant[:40], exc)
        return {}

def lookup_parcel(owner: str) -> dict:
    if not owner:
        return {}
    cache_key = owner.strip().upper()
    if cache_key in _parcel_cache:
        return _parcel_cache[cache_key]
    result: dict = {}
    for variant in name_variants(owner):
        result = _socrata_lookup(variant)
        if result.get("prop_address") or result.get("mail_address"):
            _parcel_cache[cache_key] = result
            return result
    for variant in name_variants(owner):
        result = _arcgis_lookup(variant)
        if result.get("prop_address") or result.get("mail_address"):
            _parcel_cache[cache_key] = result
            return result
    result = _socrata_fuzzy(owner.strip().upper())
    _parcel_cache[cache_key] = result
    return result


# ==============================================================================
#  CLERK PORTAL
# ==============================================================================

def build_search_url(doc_type: str, date_from: str, date_to: str) -> str:
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
        if not isinstance(hit, dict):
            continue
        src = hit.get("_source") or hit
        def g(*keys) -> str:
            for k in keys:
                v = src.get(k)
                if v:
                    return safe_str(v)
            return ""
        doc_num   = g("instrumentNumber", "docNumber", "instrument_number", "InstrumentNumber")
        filed     = g("recordedDate", "filedDate", "recorded_date", "RecordedDate")
        owner     = g("grantor", "grantors", "owner", "Grantor")
        grantee   = g("grantee", "grantees", "Grantee")
        legal     = g("legalDescription", "legal_description", "legal", "Legal")
        amount    = g("consideration", "amount", "Amount", "Consideration")
        dtype     = g("documentType", "doc_type", "DocumentType") or doc_type
        doc_id    = hit.get("id") or hit.get("_id") or hit.get("documentId") or ""
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
    m = re.search(r"(\d{4}-\d{5,}|\d{8,})", text)
    if m:
        row["document number"] = m.group(1)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if m:
        row["filed"] = m.group(1)
    m = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
    if m:
        row["amount"] = m.group(0)
    m = re.search(r"Grantor[:\s]+([^\n|]+)", text, re.I)
    if m:
        row["grantor"] = m.group(1).strip()
    m = re.search(r"Grantee[:\s]+([^\n|]+)", text, re.I)
    if m:
        row["grantee"] = m.group(1).strip()
    return row

def _row_to_record(row: dict, href: str, doc_type: str) -> dict:
    def g(*keys) -> str:
        for k in keys:
            v = (row.get(k, "") or row.get(k.lower(), "") or
                 row.get(k.upper(), ""))
            if v:
                return safe_str(v)
        return ""
    return {
        "doc_num":   g("document number", "doc number", "instrument",
                       "doc #", "doc_num"),
        "doc_type":  g("document type", "type", "doc type",
                       "documenttype") or doc_type,
        "filed":     _normalise_date(g("filed", "file date", "recorded",
                                       "date filed", "date")),
        "owner":     g("grantor", "owner", "grantors"),
        "grantee":   g("grantee", "grantees"),
        "legal":     g("legal", "legal description", "description",
                       "legaldescription"),
        "amount":    g("amount", "consideration", "debt", "lien amount"),
        "clerk_url": href,
    }

async def _parse_neumo_html(page: Page, doc_type: str) -> list[dict]:
    records: list[dict] = []
    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table:
        headers = [th.get_text(" ", strip=True).lower()
                   for th in table.find_all("th")]
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells:
                continue
            link = tr.find("a", href=True)
            href = _abs_url(link["href"]) if link else ""
            records.append(_row_to_record(
                dict(zip(headers, cells)), href, doc_type))
        if records:
            return records
    for sel in [
        "[data-testid='result-item']",
        "[class*='record-card']",
        "[class*='result-item']",
        "[class*='result-card']",
        "[class*='search-result']",
        "li[class*='result']",
        "div[class*='ResultItem']",
        "div[class*='RecordItem']",
        "div[class*='Hit']",
        "article",
    ]:
        cards = soup.select(sel)
        if cards:
            for card in cards:
                link = card.find("a", href=True)
                href = _abs_url(link["href"]) if link else ""
                row  = _text_to_row(card.get_text(" ", strip=True))
                rec  = _row_to_record(row, href, doc_type)
                if not rec["doc_num"] and href:
                    m = re.search(r"/doc/([^/?#]+)", href)
                    if m:
                        rec["doc_num"] = m.group(1)
                if rec["doc_num"] or rec["owner"]:
                    records.append(rec)
            if records:
                return records
    return records

async def _click_next(page: Page) -> bool:
    btn = page.locator(
        "button:has-text('Next'), [aria-label='Next page'], "
        "[data-testid='next-page'], button[class*='next' i], "
        "a:has-text('Next')"
    ).first
    if await btn.count() == 0:
        return False
    disabled = await btn.get_attribute("disabled")
    cls = (await btn.get_attribute("class") or "").lower()
    if disabled is not None or "disabled" in cls:
        return False
    await btn.click()
    await asyncio.sleep(2.5)
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

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if "json" in ct and any(x in response.url
                                for x in ["/api/", "/search",
                                          "/result", "/record"]):
            try:
                api_responses.append(await response.json())
            except Exception:
                pass

    page.on("response", capture)
    try:
        url = build_search_url(doc_type, date_from, date_to)
        log.info("  %s", url)
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                await page.goto(url, timeout=60_000, wait_until="networkidle")
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS:
                    raise
                log.warning("    nav attempt %d: %s", attempt, exc)
                await asyncio.sleep(RETRY_DELAY)
        await asyncio.sleep(4)
        title = await page.title()
        if "Loading" in title:
            for _ in range(35):
                await asyncio.sleep(1)
                title = await page.title()
                if "Loading" not in title:
                    break
        if doc_type == "LP":
            await screenshot(page, "search_LP")
            await save_html(page, "search_LP")
        log.info("  title: %s | api: %d", title, len(api_responses))
        if api_responses:
            for body in api_responses:
                records.extend(_extract_from_api(body, doc_type))
            if records:
                if DEBUG and api_responses:
                    try:
                        (DEBUG_DIR / f"api_{doc_type}.json").write_text(
                            json.dumps(api_responses[0],
                                       indent=2, default=str)[:50000])
                    except Exception:
                        pass
                for _ in range(20):
                    api_responses.clear()
                    if not await _click_next(page):
                        break
                    for body in api_responses:
                        records.extend(_extract_from_api(body, doc_type))
        if not records:
            records = await _parse_neumo_html(page, doc_type)
            for _ in range(20):
                if not await _click_next(page):
                    break
                new = await _parse_neumo_html(page, doc_type)
                if not new:
                    break
                records.extend(new)
        log.info("  -> %d records for %s", len(records), doc_type)
    except Exception as exc:
        log.error("scrape_doc_type(%s): %s", doc_type, exc)
        if DEBUG:
            await screenshot(page, f"error_{doc_type}")
    finally:
        await page.close()
    return records

async def run_clerk_scrape(date_from: str, date_to: str) -> list[dict]:
    all_records: list[dict] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1280,900",
            ],
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
                "Accept": ("text/html,application/xhtml+xml,"
                           "application/xml;q=0.9,*/*;q=0.8"),
            },
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages',
                                  {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
        """)
        warmup = await context.new_page()
        try:
            await warmup.goto(CLERK_BASE, timeout=30_000,
                              wait_until="networkidle")
            await asyncio.sleep(3)
            await screenshot(warmup, "homepage")
        except Exception as exc:
            log.warning("Warmup non-fatal: %s", exc)
        finally:
            await warmup.close()
        for code in DOC_TYPE_MAP:
            try:
                recs = await scrape_doc_type(
                    context, code, date_from, date_to)
                for r in recs:
                    r["cat"]       = code
                    r["cat_label"] = DOC_TYPE_MAP[code][1]
                all_records.extend(recs)
            except Exception as exc:
                log.error("Failed %s: %s", code, exc)
            await asyncio.sleep(2)
        await browser.close()
    return all_records


# ==============================================================================
#  SCORING
# ==============================================================================

def compute_flags(rec: dict, today: datetime) -> list[str]:
    flags: list[str] = list(DOC_TYPE_MAP.get(rec["cat"], ("", "", []))[2])
    owner_up = rec.get("owner", "").upper()
    if any(kw in owner_up for kw in
           ["LLC", "INC", "CORP", "LTD", "L.L.C",
            "TRUST", "HOLDINGS", "PROPERTIES"]):
        flags.append("LLC / corp owner")
    try:
        filed_dt = datetime.strptime(rec.get("filed", ""), "%m/%d/%Y")
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


# ==============================================================================
#  ASSEMBLE + SAVE
# ==============================================================================

def assemble_records(raw_records: list[dict], today: datetime) -> list[dict]:
    assembled: list[dict] = []
    seen: set = set()
    total = len(raw_records)

    for i, raw in enumerate(raw_records, 1):
        try:
            doc_num = safe_str(raw.get("doc_num", ""))
            owner   = safe_str(raw.get("owner", ""))
            filed   = safe_str(raw.get("filed", ""))
            cat     = safe_str(raw.get("cat", ""))

            # Deduplicate: use doc_num when available,
            # otherwise use owner+filed+cat as a composite key
            if doc_num:
                dedup_key = ("doc", doc_num)
            else:
                dedup_key = ("combo", owner.upper(), filed, cat)

            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            amount_str = safe_str(raw.get("amount", ""))
            amount_raw = parse_amount(amount_str)

            if i % 20 == 0:
                log.info("  Parcel lookup %d/%d (cache: %d)",
                         i, total, len(_parcel_cache))
            parcel = lookup_parcel(owner)

            rec: dict = {
                "doc_num":      doc_num,
                "doc_type":     safe_str(raw.get("doc_type", "")),
                "filed":        filed,
                "cat":          cat,
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
    log.info("Assembled %d records, %d with address", len(assembled), with_addr)
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
        log.info("Saved -> %s", path)


def export_ghl_csv(records: list[dict]) -> None:
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
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "TX"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "TX"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat", ""),
                "Document Type":          r.get("cat_label", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Collin County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV -> %s", out_path)


# ==============================================================================
#  ENTRY POINT
# ==============================================================================

async def main() -> None:
    today = datetime.now()
    start = today - timedelta(days=LOOKBACK_DAYS)
    fmt = "%-m/%-d/%Y" if sys.platform != "win32" else "%#m/%#d/%Y"
    date_from = start.strftime(fmt)
    date_to   = today.strftime(fmt)
    log.info("=" * 60)
    log.info("Collin County Motivated Seller Scraper")
    log.info("Range: %s -> %s", date_from, date_to)
    log.info("=" * 60)
    raw_records = await run_clerk_scrape(date_from, date_to)
    log.info("Raw records from clerk: %d", len(raw_records))
    records = assemble_records(raw_records, today)
    save_output(records, date_from, date_to)
    export_ghl_csv(records)
    log.info("Complete. %d records, %d with address.",
             len(records), sum(1 for r in records if r.get("prop_address")))


if __name__ == "__main__":
    asyncio.run(main())
