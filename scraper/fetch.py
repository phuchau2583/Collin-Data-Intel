#!/usr/bin/env python3
"""
Collin County, TX — motivated seller lead scraper.

Pipeline
--------
1. Clerk portal (Playwright, async): pull Official Public Records filings for the
   last LOOKBACK_DAYS days, for every distressed document type we care about.
2. Property appraiser (requests + BeautifulSoup): download the CollinCAD bulk
   parcel export (handles ASP.NET __doPostBack downloads), read the DBF with
   dbfread, and build an owner-name -> property/mailing address index.
3. Join, flag, score, and write dashboard/records.json + data/records.json.
4. Optional GHL (GoHighLevel) CSV export.

Design rules baked in here:
  * every network call is retried (RETRIES attempts, exponential backoff);
  * every per-record parse is wrapped — one bad row never kills the run;
  * a failed clerk run or a failed CAD download still produces valid output JSON.

Usage
-----
    python scraper/fetch.py                       # full run
    python scraper/fetch.py --lookback 14         # wider window
    python scraper/fetch.py --no-parcels          # skip the CAD bulk download
    python scraper/fetch.py --no-foreclosures     # skip the foreclosure-notice app
    python scraper/fetch.py --export-csv out.csv  # extra copy of the GHL export
    python scraper/fetch.py --debug               # verbose + headful browser
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"

for _d in (DASHBOARD_DIR, DATA_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Configuration (every value overridable by environment variable)
# ---------------------------------------------------------------------------

CLERK_PORTAL_URL = os.getenv("CLERK_PORTAL_URL", "https://www.collincountytx.gov/county-clerk")
# Direct URL of the public records SEARCH application. Leave blank to let the
# scraper discover it from the clerk landing page (see discover_clerk_search_url).
CLERK_SEARCH_URL = os.getenv("CLERK_SEARCH_URL", "").strip()

# Collin County publishes notices of foreclosure sale in its own app rather than
# in the clerk's real-property index (verified: zero trustee-sale filings appear
# under the clerk's NOTICE document type).
FORECLOSURE_URL = os.getenv(
    "FORECLOSURE_URL", "https://apps2.collincountytx.gov/ForeclosureNotices")

CAD_BASE_URL = os.getenv("CAD_BASE_URL", "https://collincad.org/")
# Direct URL of a .zip/.dbf bulk parcel export, if you already know it.
CAD_DBF_URL = os.getenv("CAD_DBF_URL", "").strip()
# Local .dbf / .zip to use instead of downloading (handy for offline testing).
CAD_LOCAL_FILE = os.getenv("CAD_LOCAL_FILE", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
RETRIES = int(os.getenv("RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "2.0"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "60000"))
MAX_PAGES_PER_TYPE = int(os.getenv("MAX_PAGES_PER_TYPE", "25"))
MAX_RECORDS_PER_TYPE = int(os.getenv("MAX_RECORDS_PER_TYPE", "1000"))
HEADLESS = os.getenv("HEADLESS", "1") != "0"

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

SOURCE_LABEL = ("Collin County Clerk Official Public Records + County Foreclosure "
                "Notices + CollinCAD parcel data")

log = logging.getLogger("collin")

# ---------------------------------------------------------------------------
# Document types -> internal categories
# ---------------------------------------------------------------------------

# code -> (category key, human label)
#
# Two families of codes live here:
#
#   * VERIFIED_CODES — the codes Collin County's PublicSearch app actually uses,
#     read straight out of its Advanced Search (see --discover-types). These are
#     what get queried by default.
#   * SPEC_ALIASES — the generic lead-type codes this project was specified with.
#     Collin does not use most of them, but they are kept so that a document
#     typed with one of these codes (or a run configured with them) still lands
#     in the right lead category.
#
# Re-derive the verified list any time with:
#     python scraper/fetch.py --discover-types "LIEN,JUDGMENT,PENDENS,PROBATE"

VERIFIED_CODES: Dict[str, Tuple[str, str]] = {
    "LP":      ("lis_pendens",   "Lis Pendens"),
    "AJ":      ("judgment",      "Abstract of Judgment"),
    "JD":      ("judgment",      "Judgment"),
    "ABSJ":    ("judgment",      "State Abstract of Judgment"),
    "LIEN":    ("lien",          "Lien"),
    "ML":      ("mechanic_lien", "Mechanic's Lien & Affidavit"),
    "PN":      ("lien",          "Paving Lien"),
    "FTL":     ("tax_lien",      "Federal Tax Lien"),
    "STL":     ("tax_lien",      "State Tax Lien"),
    "PRSTLI":  ("tax_lien",      "Individual State Tax Lien"),
    "CSL":     ("lien",          "Child Support Lien"),
    "PROB":    ("probate",       "Probate Document"),
    "AH":      ("probate",       "Affidavit of Heirship"),
    "DB":      ("probate",       "Certified Copy of Death Certificate"),
    # Collin has no dedicated foreclosure type: notices of substitute trustee's
    # sale are filed as a generic NOTICE, ~250 a week, almost all of them builder
    # and municipal paperwork. They are pulled and then filtered down to the
    # foreclosure-related ones by FORECLOSURE_PARTY_PATTERN.
    "NTC":     ("notice",        "Notice"),
}

SPEC_ALIASES: Dict[str, Tuple[str, str]] = {
    "NOFC":     ("foreclosure",   "Notice of Foreclosure"),
    "TAXDEED":  ("tax_deed",      "Tax Deed"),
    "JUD":      ("judgment",      "Judgment"),
    "CCJ":      ("judgment",      "Certified Judgment"),
    "DRJUD":    ("judgment",      "Domestic Relations Judgment"),
    "LNCORPTX": ("tax_lien",      "Corporate Tax Lien"),
    "LNIRS":    ("tax_lien",      "IRS Lien"),
    "LNFED":    ("tax_lien",      "Federal Lien"),
    "LN":       ("lien",          "Lien"),
    "LNMECH":   ("mechanic_lien", "Mechanic's Lien"),
    "LNHOA":    ("hoa_lien",      "HOA Lien"),
    "MEDLN":    ("medicaid_lien", "Medicaid Lien"),
    "PRO":      ("probate",       "Probate Document"),
    "NOC":      ("noc",           "Notice of Commencement"),
    "RELLP":    ("release_lp",    "Release of Lis Pendens"),
}

DOC_TYPES: Dict[str, Tuple[str, str]] = dict(VERIFIED_CODES)
for _code, _meta in SPEC_ALIASES.items():
    DOC_TYPES.setdefault(_code, _meta)

# Queried by default (one search per code, so keep it to codes the portal knows).
# NTC is excluded: a 30-day sweep produced 753 notices and zero foreclosures, and
# real notices of foreclosure sale come from the county's own notice app instead.
# Add it back with --types or CLERK_DOC_TYPES if the county starts recording them.
DEFAULT_QUERY_CODES: List[str] = [code for code in VERIFIED_CODES if code != "NTC"]

# Free-text fallbacks: portals that don't accept a code still print a description.
# Used to classify rows whose doc-type column is a phrase, not a code.
DESCRIPTION_PATTERNS: Sequence[Tuple[str, str]] = (
    (r"release.*lis\s*pendens",                          "RELLP"),
    (r"lis\s*pendens",                                   "LP"),
    (r"(notice\s+of\s+)?(substitute\s+)?trustee'?s?\s+sale|notice\s+of\s+(substitute\s+)?"
     r"trustee|notice\s+of\s+foreclosure|foreclosure",   "NOFC"),
    (r"tax\s+(deed|sale)|sheriff'?s?\s+deed|constable'?s?\s+deed",  "TAXDEED"),
    (r"affidavit\s+of\s+heirship|heirship",              "AH"),
    (r"death\s+certif",                                  "DB"),
    (r"probate|letters\s+testamentary|small\s+estate|last\s+will|will\s+and\s+testament",
                                                          "PROB"),
    (r"domestic\s+relations?\s+judgment|divorce\s+judgment",        "DRJUD"),
    (r"(state\s+)?abstract\s+of\s+judgment",            "AJ"),
    (r"certified\s+judgment",                            "CCJ"),
    (r"judgment",                                        "JD"),
    (r"(irs|internal\s+revenue)\s+lien|federal\s+tax\s+lien|federal\s+lien",  "FTL"),
    (r"(state|corporate|corp)\s+tax\s+lien|comptroller", "STL"),
    (r"child\s+support\s+lien",                          "CSL"),
    (r"medicaid\s+lien|estate\s+recovery",               "MEDLN"),
    (r"mechanic'?s?\s*(and\s+materialman'?s?)?\s*lien|m\s*&\s*m\s+lien",     "ML"),
    (r"paving\s+lien",                                   "PN"),
    (r"(hoa|homeowners?\s+association|assessment)\s+lien",           "LNHOA"),
    (r"notice\s+of\s+commencement",                      "NOC"),
    (r"\blien\b",                                       "LIEN"),
)

CORP_PATTERN = re.compile(
    r"\b(LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO\.|LP|L\.P|LLP|LTD|"
    r"TRUST|TRUSTEE|HOLDINGS?|PROPERTIES|PARTNERS(HIP)?|ENTERPRISES?|INVESTMENTS?|"
    r"VENTURES?|GROUP|ASSOCIATION|ESTATE\s+OF|BANK|CREDIT\s+UNION|MORTGAGE)\b",
    re.I,
)

# Party names that are never real leads (plaintiffs, agencies, banks filing against owners).
NOISE_NAME_PATTERN = re.compile(
    r"^(STATE\s+OF\s+TEXAS|UNITED\s+STATES|INTERNAL\s+REVENUE|IRS\b|COUNTY\s+OF\b|"
    r"CITY\s+OF\b|COLLIN\s+COUNTY\b|TEXAS\s+(WORKFORCE|COMPTROLLER)|UNKNOWN|N/?A)$",
    re.I,
)

# Substitute trustees and the firms that post Texas foreclosure sales. A generic
# NOTICE filing is only a pre-foreclosure lead when one of these is a party.
FORECLOSURE_PARTY_PATTERN = re.compile(
    r"\b(SUBSTITUTE\s+TRUSTEE|SUBS?\s+TRUSTEE|TRUSTEE'?S?\s+SALE|FORECLOSURE|"
    r"MACKIE\s+WOLF|BARRETT\s+DAFFIN|HUGHES\s+WATTERS|CODILIS|MARINOSCI|POWER\s+DEFAULT|"
    r"AUCTION\.COM|SETTLEPOU|TIFFANY\s*(&|AND)\s*BOSCO|ROBERTSON\s+ANSCHUTZ|"
    r"SHAPIRO\s+SCHWARTZ|MILLER\s+GEORGE|ALDRIDGE\s+PITE|LOGS\s+LEGAL|"
    r"SUBSTITUTE\s+TRUSTEE\s+SERVICES|FORECLOSURE\s+SERVICES?)\b", re.I)

# Parties that file *against* an owner: banks, servicers, law firms, HOAs, agencies.
INSTITUTION_PATTERN = re.compile(
    r"\b(BANK|BANKING|MORTGAGE|LENDING|LOANS?|CREDIT\s+UNION|SAVINGS|SERVICING|CAPITAL|"
    r"FINANCIAL|FINANCE|TITLE\s+(CO|COMPANY|INSURANCE)|ESCROW|TRUSTEE|SUBSTITUTE\s+TRUSTEE|"
    r"ATTORNEYS?|LAW\s+(FIRM|OFFICES?|GROUP)|LLP|PLLC|ASN|ASSOCIATION|HOA|HOMEOWNERS|"
    r"CONDOMINIUM|MUNICIPAL|UTILITY\s+DISTRICT|MUD\b|ISD\b|CITY\s+OF|COUNTY\s+OF|"
    r"STATE\s+OF|UNITED\s+STATES|INTERNAL\s+REVENUE|IRS\b|COMPTROLLER|WORKFORCE|"
    r"DEPARTMENT|COMMISSION|AUTHORITY|MERS\b|"
    r"MORTGAGE\s+ELECTRONIC|NOMINEE)\b", re.I)

SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "MD", "DDS", "ESQ", "PHD"}


def is_institution(name: Any) -> bool:
    """True for banks, HOAs, agencies and other parties that are never the lead."""
    text = normalize_name(name)
    if not text:
        return True
    return bool(INSTITUTION_PATTERN.search(text) or NOISE_NAME_PATTERN.match(text))

# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def date_range(lookback_days: int) -> Tuple[datetime, datetime]:
    end = utcnow()
    start = end - timedelta(days=lookback_days)
    return start, end


def clean(value: Any) -> str:
    """Collapse whitespace, strip NBSP junk, return '' for None."""
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\xa0", " ").replace("​", "")
    return re.sub(r"\s+", " ", text).strip()


def parse_amount(value: Any) -> Optional[float]:
    """'$1,234.56' / '1234' / '' -> float | None. Never raises."""
    try:
        text = clean(value)
        if not text:
            return None
        negative = text.startswith("(") and text.endswith(")")
        text = re.sub(r"[^0-9.\-]", "", text)
        if not text or text in {"-", ".", "-."}:
            return None
        amount = float(text)
        return -amount if negative else amount
    except Exception:  # noqa: BLE001 - never crash on a bad cell
        return None


DATE_FORMATS = (
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%d-%b-%Y",
    "%b %d, %Y", "%B %d, %Y", "%Y%m%d", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
)


def parse_date(value: Any) -> Optional[datetime]:
    """Best-effort date parse. Returns naive UTC-ish datetime or None."""
    text = clean(value)
    if not text:
        return None
    if isinstance(value, datetime):
        return value
    match = re.search(r"\d{1,4}[/-][A-Za-z0-9]{1,3}[/-]\d{2,4}|\d{8}", text)
    candidate = match.group(0) if match else text
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def iso_date(value: Any) -> str:
    parsed = parse_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else clean(value)


def retry(func, *args, attempts: int = RETRIES, label: str = "", **kwargs):
    """Run func with N attempts and exponential backoff. Returns None on failure."""
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - retry anything transient
            last_error = exc
            wait = RETRY_BACKOFF ** attempt
            log.warning("%s attempt %d/%d failed: %s (retrying in %.1fs)",
                        label or getattr(func, "__name__", "call"), attempt, attempts, exc, wait)
            if attempt < attempts:
                time.sleep(wait)
    log.error("%s failed after %d attempts: %s", label or "call", attempts, last_error)
    return None


async def aretry(coro_factory, attempts: int = RETRIES, label: str = ""):
    """Async twin of retry(). coro_factory must be a zero-arg callable."""
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await coro_factory()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            wait = RETRY_BACKOFF ** attempt
            log.warning("%s attempt %d/%d failed: %s (retrying in %.1fs)",
                        label, attempt, attempts, exc, wait)
            if attempt < attempts:
                await asyncio.sleep(wait)
    log.error("%s failed after %d attempts: %s", label, attempts, last_error)
    return None


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


# ---------------------------------------------------------------------------
# Owner-name normalization
# ---------------------------------------------------------------------------

NAME_SPLIT_PATTERN = re.compile(r"\s+(?:&|AND)\s+|\s*;\s*|\s*\|\s*", re.I)


def normalize_name(raw: Any) -> str:
    """Upper-case, drop punctuation/suffixes, collapse spaces."""
    text = clean(raw).upper()
    if not text:
        return ""
    text = re.sub(r"\b(ET\s+(AL|UX|VIR)|C/O|DBA|AKA|FKA|TRUSTEE|TRS|TR)\b\.?", " ", text)
    text = re.sub(r"[^A-Z0-9,& ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,&")
    return text


def split_parties(raw: Any) -> List[str]:
    """'SMITH JOHN & SMITH JANE' -> ['SMITH JOHN', 'SMITH JANE']."""
    text = clean(raw)
    if not text:
        return []
    parts = [p.strip() for p in NAME_SPLIT_PATTERN.split(text) if p and p.strip()]
    return parts or [text]


def name_keys(raw: Any) -> Set[str]:
    """
    Every reasonable spelling of one person's name, normalized to a lookup key.

    Covers the three orderings required by the spec — "FIRST LAST",
    "LAST FIRST" and "LAST, FIRST" — plus first+last-only variants so that a
    middle name or initial on one side of the join doesn't lose the match.
    Corporate names index as a single key.
    """
    keys: Set[str] = set()
    name = normalize_name(raw)
    if not name or NOISE_NAME_PATTERN.match(name):
        return keys

    keys.add(name.replace(",", " ").replace("  ", " ").strip())

    if CORP_PATTERN.search(name):
        # Companies don't get re-ordered; just index the cleaned string.
        return {k for k in keys if k}

    comma_form = "," in name
    body = name.replace(",", " ")
    tokens = [t for t in body.split() if t and t not in SUFFIXES]
    if len(tokens) < 2:
        return {k for k in keys if k}

    if comma_form:
        last_part, _, rest = name.partition(",")
        last = normalize_name(last_part)
        firsts = [t for t in normalize_name(rest).split() if t not in SUFFIXES]
    else:
        # Ambiguous ordering — index both readings.
        last, firsts = tokens[0], tokens[1:]
        keys.add(" ".join(tokens[1:] + [tokens[0]]))
        keys.add(" ".join([tokens[-1]] + tokens[:-1]))

    if last and firsts:
        first = firsts[0]
        keys.add(" ".join([last] + firsts))          # LAST FIRST MIDDLE
        keys.add(" ".join(firsts + [last]))          # FIRST MIDDLE LAST
        keys.add(f"{last} {first}")                  # LAST FIRST
        keys.add(f"{first} {last}")                  # FIRST LAST
        keys.add(f"{last}, {first}".replace(",", ""))  # LAST, FIRST (normalized)
    return {k.strip() for k in keys if k and len(k) > 3}


def split_person_name(raw: Any) -> Tuple[str, str]:
    """Return (first_name, last_name) for the GHL export. Corps -> ('', full)."""
    name = normalize_name(raw)
    if not name:
        return "", ""
    if CORP_PATTERN.search(name):
        return "", clean(raw).upper()
    if "," in name:
        last, _, rest = name.partition(",")
        firsts = [t for t in rest.split() if t not in SUFFIXES]
        return (firsts[0] if firsts else ""), last.strip()
    tokens = [t for t in name.split() if t not in SUFFIXES]
    if len(tokens) == 1:
        return "", tokens[0]
    # County indexes are LAST FIRST MIDDLE; that's the safer default here.
    return tokens[1], tokens[0]


# ---------------------------------------------------------------------------
# CollinCAD bulk parcel data
# ---------------------------------------------------------------------------

# Column name candidates, in priority order. The first block of each tuple is
# the generic CAMA naming from the spec; the rest are the names CollinCAD
# actually ships in parcels_with_appraisal_data (ESRI shapefile .dbf, so field
# names are truncated to 10 characters).
COL_OWNER = ("OWNER", "OWN1", "OWNER_NAME", "OWNERNAME", "NAME", "OWNER1", "PY_OWNER",
             "FILE_AS_NA", "OWNER_NAM", "PY_OWNER_N")
COL_OWNER2 = ("OWN2", "OWNER2", "OWNER_NAME2", "DBA_NAME")
COL_SITE_ADDR = ("SITE_ADDR", "SITEADDR", "SITUS", "SITUS_ADDR", "PROP_ADDR", "SITE_ADD",
                 "LOCATION", "SITUS_DISP")
COL_SITE_CITY = ("SITE_CITY", "SITECITY", "SITUS_CITY", "PROP_CITY")
COL_SITE_STATE = ("SITE_STATE", "SITUS_STAT", "SITUS_STATE")
COL_SITE_ZIP = ("SITE_ZIP", "SITEZIP", "SITUS_ZIP", "PROP_ZIP")
COL_MAIL_ADDR = ("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILADDR", "ADDR1", "MAIL_ADDR1",
                 "ADDRESS1", "ADDR_LINE1")
COL_MAIL_ADDR2 = ("ADDR_2", "MAILADR2", "ADDR2", "MAIL_ADDR2", "ADDRESS2", "ADDR_LINE2")
COL_MAIL_ADDR3 = ("ADDR_3", "ADDR3", "ADDRESS3", "ADDR_LINE3")
COL_MAIL_CITY = ("MAILCITY", "MAIL_CITY", "ADDR_CITY", "CITY")
COL_MAIL_STATE = ("MAILSTATE", "MAIL_STATE", "ADDR_STATE", "STATE", "ST")
COL_MAIL_ZIP = ("MAILZIP", "MAIL_ZIP", "ADDR_ZIP", "ZIP", "ZIPCODE")
# CollinCAD splits the situs address across these when there is no display field.
COL_SITUS_PARTS = ("SITUS_NUM", "SITUS_STRE", "SITUS_ST_1", "SITUS_ST_2")


def pick(record: Dict[str, Any], candidates: Sequence[str]) -> str:
    """First non-empty value among candidate column names (case-insensitive)."""
    for candidate in candidates:
        for key in (candidate, candidate.lower(), candidate.title()):
            if key in record:
                value = clean(record[key])
                if value:
                    return value
    upper = {str(k).upper(): v for k, v in record.items()}
    for candidate in candidates:
        value = clean(upper.get(candidate.upper()))
        if value:
            return value
    return ""


COLUMN_GROUPS: Dict[str, Sequence[str]] = {
    "owner": COL_OWNER, "owner2": COL_OWNER2,
    "site_addr": COL_SITE_ADDR, "site_city": COL_SITE_CITY,
    "site_state": COL_SITE_STATE, "site_zip": COL_SITE_ZIP,
    "mail1": COL_MAIL_ADDR, "mail2": COL_MAIL_ADDR2, "mail3": COL_MAIL_ADDR3,
    "mail_city": COL_MAIL_CITY, "mail_state": COL_MAIL_STATE, "mail_zip": COL_MAIL_ZIP,
}


def resolve_columns(field_names: Iterable[str]) -> Dict[str, Any]:
    """
    Resolve our column roles against one table's field names, once.

    A 440k-row parcel table is far too big to re-probe candidate column names on
    every record, so the whole schema is pinned down up front.
    """
    available = {str(name).upper(): name for name in field_names}
    columns: Dict[str, Any] = {}
    for role, candidates in COLUMN_GROUPS.items():
        for candidate in candidates:
            actual = available.get(candidate.upper())
            if actual is not None:
                columns[role] = actual
                break
    columns["situs_parts"] = [available[part] for part in COL_SITUS_PARTS if part in available]
    return columns


def parcel_entry_mapped(record: Dict[str, Any], columns: Dict[str, Any]) -> Dict[str, str]:
    """parcel_entry() using a pre-resolved column map. Never raises."""
    def value(role: str) -> str:
        column = columns.get(role)
        return clean(record.get(column)) if column else ""

    site_column = columns.get("site_addr")
    raw_site = record.get(site_column) if site_column else ""
    # situs_disp is "3813 ARIZONA PL \r\nPLANO, TX 75023" — keep the street line
    # only, and split before clean() collapses the newline into a space.
    site = clean(str(raw_site).replace("\r", "\n").split("\n")[0]) if raw_site else ""
    if not site:
        site = clean(" ".join(filter(None, (clean(record.get(part))
                                            for part in columns.get("situs_parts", [])))))
    mail = clean(", ".join([line for line in
                            (value("mail1"), value("mail2"), value("mail3")) if line]))
    return {
        "prop_address": site,
        "prop_city": value("site_city"),
        "prop_state": value("site_state") or ("TX" if site else ""),
        "prop_zip": value("site_zip")[:10],
        "mail_address": mail,
        "mail_city": value("mail_city"),
        "mail_state": value("mail_state") or ("TX" if mail else ""),
        "mail_zip": value("mail_zip")[:10],
    }


def tidy_name(raw: Any) -> str:
    """Trim the dangling separators CAD owner strings carry ('HUGHES HAROLD G &')."""
    text = clean(raw)
    text = re.sub(r"\s*[&,;/]\s*$", "", text)
    return re.sub(r"\s+(AND|&)\s*$", "", text, flags=re.I).strip()


def normalize_address(raw: Any) -> str:
    """'1000 Manchester Dr.' -> '1000 MANCHESTER DR' for address joins."""
    text = clean(raw).upper()
    if not text:
        return ""
    text = re.sub(r"[.,#]", " ", text)
    text = re.sub(r"\b(APT|UNIT|STE|SUITE|#)\s*\w+$", "", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def zip5(raw: Any) -> str:
    match = re.search(r"\d{5}", clean(raw))
    return match.group(0) if match else ""


class ParcelIndex:
    """CollinCAD parcels, searchable by owner name and by property address."""

    def __init__(self) -> None:
        self.by_owner: Dict[str, Dict[str, str]] = {}
        self.by_address: Dict[str, Dict[str, str]] = {}
        self.rows = 0

    def __bool__(self) -> bool:
        return bool(self.by_owner or self.by_address)

    def __len__(self) -> int:
        return len(self.by_owner)

    def add(self, owner_raw: str, entry: Dict[str, str]) -> None:
        for party in split_parties(owner_raw):
            for key in name_keys(party):
                self.by_owner.setdefault(key, entry)
        address = normalize_address(entry.get("prop_address"))
        if address:
            owned = dict(entry, owner=tidy_name(owner_raw))
            code = zip5(entry.get("prop_zip"))
            if code:
                self.by_address.setdefault(f"{address}|{code}", owned)
            self.by_address.setdefault(address, owned)

    def owner_lookup(self, owner: Any) -> Dict[str, str]:
        if not self.by_owner or not owner:
            return {}
        for party in split_parties(owner):
            for key in name_keys(party):
                hit = self.by_owner.get(key)
                if hit:
                    return hit
        return {}

    def address_lookup(self, address: Any, postal: Any = "") -> Dict[str, str]:
        if not self.by_address:
            return {}
        key = normalize_address(address)
        if not key:
            return {}
        code = zip5(postal)
        return (self.by_address.get(f"{key}|{code}") if code else None) \
            or self.by_address.get(key) or {}


def parcel_entry(record: Dict[str, Any]) -> Dict[str, str]:
    """One parcel row -> the address fields we publish. Never raises."""
    site = ""
    for candidate in COL_SITE_ADDR:  # split before clean() flattens the newline
        for key in (candidate, candidate.lower(),
                    next((k for k in record if str(k).upper() == candidate.upper()), None)):
            if key and key in record and record[key]:
                site = clean(str(record[key]).replace("\r", "\n").split("\n")[0])
                break
        if site:
            break
    if not site:
        site = clean(" ".join(filter(None, (pick(record, (part,)) for part in COL_SITUS_PARTS))))

    mail_lines = [pick(record, group) for group in
                  (COL_MAIL_ADDR, COL_MAIL_ADDR2, COL_MAIL_ADDR3)]
    mail = clean(", ".join([line for line in mail_lines if line]))

    return {
        "prop_address": site,
        "prop_city": pick(record, COL_SITE_CITY),
        "prop_state": pick(record, COL_SITE_STATE) or ("TX" if site else ""),
        "prop_zip": pick(record, COL_SITE_ZIP)[:10],
        "mail_address": mail,
        "mail_city": pick(record, COL_MAIL_CITY),
        "mail_state": pick(record, COL_MAIL_STATE) or ("TX" if mail else ""),
        "mail_zip": pick(record, COL_MAIL_ZIP)[:10],
    }


def _looks_like_dbf(blob: bytes) -> bool:
    return len(blob) > 32 and blob[0] in (0x03, 0x04, 0x05, 0x30, 0x31, 0x32, 0x83, 0x8B, 0xF5)


def _looks_like_zip(blob: bytes) -> bool:
    return blob[:2] == b"PK"


def _aspnet_form_state(soup: BeautifulSoup) -> Dict[str, str]:
    """Pull __VIEWSTATE & friends out of an ASP.NET WebForms page."""
    state: Dict[str, str] = {}
    for field in soup.find_all("input"):
        name = field.get("name")
        if not name:
            continue
        if name.startswith("__") or field.get("type") in ("hidden",):
            state[name] = field.get("value", "") or ""
    return state


DOPOSTBACK_RE = re.compile(r"__doPostBack\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*\)")


def _download_via_postback(session: requests.Session, page_url: str, soup: BeautifulSoup,
                           target: str, argument: str) -> Optional[bytes]:
    """Replay a javascript:__doPostBack(...) link as a real ASP.NET form POST."""
    form = soup.find("form")
    action = (form.get("action") if form else None) or page_url
    if action.startswith("./"):
        action = action[2:]
    post_url = requests.compat.urljoin(page_url, action)

    payload = _aspnet_form_state(soup)
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument

    response = session.post(
        post_url, data=payload, timeout=HTTP_TIMEOUT, allow_redirects=True,
        headers={"Referer": page_url, "Content-Type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    blob = response.content
    content_type = response.headers.get("Content-Type", "").lower()
    if _looks_like_zip(blob) or _looks_like_dbf(blob) or "octet-stream" in content_type \
            or "zip" in content_type or "download" in response.headers.get("Content-Disposition", "").lower():
        return blob
    log.debug("Postback %s returned %s (%d bytes) — not a file", target, content_type, len(blob))
    return None


CAD_DOWNLOAD_HINTS = re.compile(
    r"(download|data\s*products?|bulk|export|appraisal\s*(roll|export)|gis|shapefile|"
    r"public\s*data|open\s*records|property\s*data)", re.I)
CAD_FILE_HINTS = re.compile(r"\.(zip|dbf)(\?|$)", re.I)


def discover_cad_file_urls(session: requests.Session, base_url: str) -> List[str]:
    """Crawl collincad.org one level deep for links to .zip/.dbf bulk exports."""
    found: List[str] = []
    pages_to_scan: List[str] = [base_url]
    seen_pages: Set[str] = set()

    def scan(url: str, depth: int) -> None:
        if url in seen_pages or depth > 2:
            return
        seen_pages.add(url)
        try:
            response = session.get(url, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.debug("CAD scan failed for %s: %s", url, exc)
            return
        soup = BeautifulSoup(response.text, "lxml")
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            text = clean(anchor.get_text())
            absolute = requests.compat.urljoin(url, href)
            if CAD_FILE_HINTS.search(href):
                if absolute not in found:
                    found.append(absolute)
            elif depth < 2 and (CAD_DOWNLOAD_HINTS.search(text) or CAD_DOWNLOAD_HINTS.search(href)):
                if absolute.startswith(("http://", "https://")) and "collincad.org" in absolute:
                    pages_to_scan.append(absolute)

    while pages_to_scan:
        nxt = pages_to_scan.pop(0)
        scan(nxt, 0 if nxt == base_url else 1)

    found.sort(key=rank_cad_url)
    return found


def rank_cad_url(url: str) -> Tuple[int, int]:
    """Lower is better. Parcel+appraisal tables beat GIS boundary layers."""
    name = url.rsplit("/", 1)[-1].lower()
    path = url.lower()
    score = 50
    if "parcel" in path:
        score -= 20
    if "appraisal" in path or "appr" in name:
        score -= 15
    if re.search(r"(owner|roll|property|prop_)", path):
        score -= 5
    if name.endswith(".dbf"):
        score -= 5
    # Boundary/reference layers carry no owner names.
    if re.search(r"(school|city_limit|countyline|abstract|subdivision|mud|condo|zip|"
                 r"precinct|tif|zoning|street|road)", name):
        score += 40
    return score, len(url)


def iter_cad_blobs(session: requests.Session) -> Iterable[Tuple[str, bytes]]:
    """
    Yield candidate CollinCAD bulk files (best first) as (label, bytes).

    CollinCAD publishes its bulk data through link.collincad.org, whose file
    pages answer a plain GET with the actual zip. Older/other ASP.NET download
    pages hand the file back only through a __doPostBack form POST, so that is
    tried too once the direct links are exhausted.
    """
    if CAD_LOCAL_FILE:
        path = Path(CAD_LOCAL_FILE)
        if path.exists():
            log.info("Using local CAD file %s", path)
            yield str(path), path.read_bytes()
        else:
            log.warning("CAD_LOCAL_FILE %s does not exist", path)

    cache_max_age = int(os.getenv("CAD_CACHE_HOURS", "20")) * 3600
    cached = CACHE_DIR / "collincad_bulk.bin"
    if cached.exists() and (time.time() - cached.stat().st_mtime) < cache_max_age:
        log.info("Using cached CAD bulk file (%.1f MB)", cached.stat().st_size / 1e6)
        yield str(cached), cached.read_bytes()

    candidates: List[str] = [CAD_DBF_URL] if CAD_DBF_URL else []
    if not candidates:
        discovered = retry(discover_cad_file_urls, session, CAD_BASE_URL,
                           label="discover CAD downloads") or []
        candidates = discovered[:10]
        log.info("Discovered %d candidate CAD file link(s)", len(candidates))

    for url in candidates:
        blob = retry(_get_binary, session, url, label=f"download {url.rsplit('/', 1)[-1]}")
        if blob and (_looks_like_zip(blob) or _looks_like_dbf(blob)):
            log.info("Downloaded %s (%.1f MB)", url, len(blob) / 1e6)
            try:
                cached.write_bytes(blob)
            except Exception as exc:  # noqa: BLE001 - cache is best-effort
                log.debug("Could not cache CAD file: %s", exc)
            yield url, blob

    blob = retry(_cad_postback_hunt, session, label="CAD __doPostBack download")
    if blob:
        try:
            cached.write_bytes(blob)
        except Exception:  # noqa: BLE001
            pass
        yield "__doPostBack", blob


def _get_binary(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=HTTP_TIMEOUT, stream=True)
    response.raise_for_status()
    return response.content


def _cad_postback_hunt(session: requests.Session) -> Optional[bytes]:
    """Look for javascript:__doPostBack download links on CAD download pages."""
    pages = [CAD_BASE_URL]
    try:
        response = session.get(CAD_BASE_URL, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        for anchor in soup.find_all("a", href=True):
            text = clean(anchor.get_text())
            if CAD_DOWNLOAD_HINTS.search(text) or CAD_DOWNLOAD_HINTS.search(anchor["href"]):
                pages.append(requests.compat.urljoin(CAD_BASE_URL, anchor["href"]))
    except Exception as exc:  # noqa: BLE001
        log.debug("CAD postback hunt bootstrap failed: %s", exc)

    for page_url in dict.fromkeys(pages):
        try:
            response = session.get(page_url, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            continue
        soup = BeautifulSoup(response.text, "lxml")
        for anchor in soup.find_all(["a", "input"]):
            raw = " ".join(filter(None, [anchor.get("href", ""), anchor.get("onclick", "")]))
            match = DOPOSTBACK_RE.search(raw)
            if not match:
                continue
            label = clean(anchor.get_text()) or anchor.get("value", "")
            if not (CAD_DOWNLOAD_HINTS.search(label) or CAD_FILE_HINTS.search(raw)
                    or re.search(r"(appraisal|parcel|owner|roll)", label, re.I)):
                continue
            log.info("Trying CAD postback download: %s (%s)", label, match.group(1))
            try:
                blob = _download_via_postback(session, page_url, soup, match.group(1), match.group(2))
            except Exception as exc:  # noqa: BLE001
                log.debug("Postback failed: %s", exc)
                continue
            if blob and (_looks_like_zip(blob) or _looks_like_dbf(blob)):
                return blob
    return None


def extract_dbf_paths(blob: bytes) -> List[Path]:
    """Write the DBF(s) from a zip (or a bare .dbf) to the cache dir; return paths."""
    out: List[Path] = []
    if _looks_like_zip(blob):
        try:
            with zipfile.ZipFile(io.BytesIO(blob)) as archive:
                members = [m for m in archive.namelist() if m.lower().endswith(".dbf")]
                # Parcel/appraisal tables first; skip tiny lookup tables later.
                members.sort(key=lambda m: (0 if re.search(r"(appr|parcel|prop|owner|roll|land)", m, re.I) else 1, m))
                for member in members[:6]:
                    target = CACHE_DIR / Path(member).name
                    with archive.open(member) as src, open(target, "wb") as dst:
                        dst.write(src.read())
                    out.append(target)
                    # Copy any sidecar memo files the DBF may reference.
                    stem = Path(member).stem
                    for sidecar in archive.namelist():
                        if Path(sidecar).stem == stem and sidecar.lower().endswith((".dbt", ".fpt")):
                            with archive.open(sidecar) as src, open(CACHE_DIR / Path(sidecar).name, "wb") as dst:
                                dst.write(src.read())
        except Exception as exc:  # noqa: BLE001
            log.error("Could not open CAD zip: %s", exc)
    elif _looks_like_dbf(blob):
        target = CACHE_DIR / "collincad_parcels.dbf"
        target.write_bytes(blob)
        out.append(target)
    return out


def build_parcel_index(session: requests.Session) -> ParcelIndex:
    """
    Load the CollinCAD bulk parcel table into a ParcelIndex.

    Owners are indexed under every name ordering ("FIRST LAST", "LAST FIRST",
    "LAST, FIRST") so a clerk grantor matches however the county typed it, and
    properties are indexed by street address so foreclosure notices — which list
    an address but no owner — can be resolved back to a person.
    """
    index = ParcelIndex()
    try:
        from dbfread import DBF  # imported late so --no-parcels works without it
    except ImportError:
        log.error("dbfread is not installed — skipping parcel enrichment (pip install dbfread)")
        return index

    for label, blob in iter_cad_blobs(session):
        paths = extract_dbf_paths(blob)
        if not paths:
            log.info("No .dbf table inside %s — trying the next candidate", label)
            continue

        for path in paths:
            try:
                table = DBF(str(path), load=False, ignore_missing_memofile=True,
                            char_decode_errors="ignore", lowernames=False)
                field_names = {str(f).upper() for f in table.field_names}
            except Exception as exc:  # noqa: BLE001
                log.warning("Could not open %s: %s", path.name, exc)
                continue

            if not (field_names & {c.upper() for c in COL_OWNER}):
                log.debug("Skipping %s — no owner column (%d fields)", path.name, len(field_names))
                continue

            log.info("Reading %s (%d fields)", path.name, len(field_names))
            columns = resolve_columns(table.field_names)
            owner_columns = [columns[role] for role in ("owner", "owner2") if role in columns]
            rows = 0
            for record in table:
                rows += 1
                try:
                    entry = parcel_entry_mapped(record, columns)
                    if not (entry["prop_address"] or entry["mail_address"]):
                        continue
                    for owner_column in owner_columns:
                        owner_raw = clean(record.get(owner_column))
                        if owner_raw:
                            index.add(owner_raw, entry)
                except Exception:  # noqa: BLE001 - one bad parcel never stops the load
                    continue
                if rows % 100000 == 0:
                    log.info("  …%d parcels, %d name keys", rows, len(index.by_owner))

            index.rows = rows
            log.info("Parcel index built from %s: %d rows, %d owner-name keys, "
                     "%d address keys", path.name, rows, len(index.by_owner),
                     len(index.by_address))
            if index:
                return index

    log.error("No usable CollinCAD parcel table — records will have no addresses")
    return index


def lookup_parcel(index: Optional[ParcelIndex], owner: str) -> Dict[str, str]:
    """Match a clerk grantor against the parcel index across all name orderings."""
    if not index or not owner:
        return {}
    return index.owner_lookup(owner)


# ---------------------------------------------------------------------------
# Clerk portal (Playwright, async)
# ---------------------------------------------------------------------------
#
# Collin County's Official Public Records live in the Kofile/GovOS "PublicSearch"
# app linked from the clerk page as "Deed Search":
#
#     https://collin.tx.publicsearch.us/
#
# Its advanced search is fully URL-drivable, and its internal document-type codes
# are the same codes this scraper is configured with, e.g.
#
#     /results?department=RP&docTypes=LP&recordedDateRange=20260827%2C20260903
#             &searchType=advancedSearch&limit=50&offset=0
#
# Results rows carry the portal's internal document id in the row checkbox
# (`table-checkbox-330496828`), which is also the permalink: /doc/330496828.
#
# If the county ever swaps vendors, scrape_clerk() falls back to a generic
# heuristic form-filling path that works with typical ASP.NET record portals.

PUBLICSEARCH_DEFAULT_URL = "https://collin.tx.publicsearch.us"
PS_DEPARTMENT = os.getenv("CLERK_DEPARTMENT", "RP")   # RP = Real Property
PS_PAGE_LIMIT = int(os.getenv("CLERK_PAGE_LIMIT", "50"))  # the app rejects >50
MAX_DETAIL_FETCH = int(os.getenv("MAX_DETAIL_FETCH", "200"))
DETAIL_DELAY_MS = int(os.getenv("DETAIL_DELAY_MS", "350"))

# Text/href signatures of the land-records search app on the clerk landing page.
SEARCH_LINK_HINTS = re.compile(
    r"(deed\s+search|official\s+public\s+records|land\s+record|real\s+property\s+record|"
    r"records?\s+search|search\s+records?|document\s+search|online\s+records)", re.I)
SEARCH_HOST_HINTS = re.compile(
    r"(publicsearch\.us|landmarkweb|tylerhost|idocmarket|govos|countyclerkrecords|"
    r"erecord|recordsearch)", re.I)
# Things that look like a search but are the wrong records (courts, jail, vitals).
SEARCH_NEGATIVE_HINTS = re.compile(
    r"(judicial|inmate|warrant|court\s+case|case\s+information|jury|vital|birth|death\s+cert|"
    r"marriage|passport|assumed\s*name|permitium|zendesk|fraudalert|historical)", re.I)

# Field-name signatures for the generic fallback path.
DATE_FROM_HINTS = re.compile(r"(from|begin|start|frm)[_ ]*date|date[_ ]*(from|begin|start)", re.I)
DATE_TO_HINTS = re.compile(r"(to|end|thru|through)[_ ]*date|date[_ ]*(to|end|thru)", re.I)
DOCTYPE_HINTS = re.compile(r"(doc(ument)?[_ ]*(type|code|desc)|instrument[_ ]*type|type[_ ]*code)", re.I)
DISCLAIMER_HINTS = re.compile(
    r"^\s*(i\s+)?(accept|agree|i\s+agree|acknowledge|continue|public\s+login|enter|"
    r"login\s+as\s+guest|guest|proceed|ok)\b", re.I)
NEXT_PAGE_HINTS = re.compile(r"^\s*(next|next\s*(page|>)|>|»|›)\s*$", re.I)

HEADER_MAP: Sequence[Tuple[str, str]] = (
    ("doc_num",   r"(instrument|document|doc|file|clerk|recording)\s*(number|num|no|#|id)|^doc#?$|^instrument$"),
    ("filed",     r"(file|filed|record(ed|ing)?)\s*date|^date$|^filed$|^recorded$"),
    ("doc_type",  r"(doc(ument)?|instrument)?\s*(type|kind|code|description)|^type$|^kind$"),
    ("owner",     r"grantor|direct\s*name|party\s*1|from\s*party|^from$|debtor|defendant"),
    ("grantee",   r"grantee|indirect\s*name|party\s*2|to\s*party|^to$|creditor|plaintiff"),
    ("legal",     r"legal|description|subdivision|abstract|lot|survey"),
    ("amount",    r"amount|consideration|debt|value|sum"),
)


def classify(doc_type_text: str, requested_code: str = "") -> Tuple[str, str, str]:
    """
    (code, category, label) for a filing, from the portal's own description text.

    Description patterns are tried before code matching: portals print phrases
    like "MECHANICS LIEN & AFFIDAVIT", and a naive code scan would see the word
    "LIEN" and file it as a generic lien.
    """
    text = clean(doc_type_text)
    if not text:
        if requested_code in DOC_TYPES:
            category, label = DOC_TYPES[requested_code]
            return requested_code, category, label
        return (requested_code or "OTHER"), "other", "Other Document"

    # An exact code (the portal sometimes prints the code itself).
    token = re.sub(r"[^A-Z0-9]", "", text.upper())
    if token in DOC_TYPES:
        category, label = DOC_TYPES[token]
        return token, category, label

    for pattern, code in DESCRIPTION_PATTERNS:
        if re.search(pattern, text, re.I):
            category, label = DOC_TYPES[code]
            return code, category, label

    if requested_code in DOC_TYPES:
        category, label = DOC_TYPES[requested_code]
        return requested_code, category, label
    return (requested_code or "OTHER"), "other", text


def discover_clerk_search_url(session: requests.Session) -> str:
    """Find the land-records search application from the clerk landing page."""
    if CLERK_SEARCH_URL:
        return CLERK_SEARCH_URL
    try:
        response = session.get(CLERK_PORTAL_URL, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
    except Exception as exc:  # noqa: BLE001
        log.error("Could not load clerk landing page (%s); using %s",
                  exc, PUBLICSEARCH_DEFAULT_URL)
        return PUBLICSEARCH_DEFAULT_URL

    scored: List[Tuple[int, str]] = []
    for anchor in soup.find_all("a", href=True):
        href = requests.compat.urljoin(CLERK_PORTAL_URL, anchor["href"].strip())
        text = clean(anchor.get_text())
        if not href.startswith(("http://", "https://")):
            continue
        blob = f"{text} {href}"
        if SEARCH_NEGATIVE_HINTS.search(blob):
            continue
        score = 0
        if SEARCH_HOST_HINTS.search(href):
            score += 4
        if SEARCH_LINK_HINTS.search(text):
            score += 2
        if SEARCH_LINK_HINTS.search(href):
            score += 1
        if score > 0:
            scored.append((score, href))

    if scored:
        scored.sort(key=lambda pair: -pair[0])
        best = scored[0][1]
        log.info("Discovered clerk records search app: %s", best)
        return best

    log.warning("No records-search link found on %s — using %s. "
                "Set CLERK_SEARCH_URL to override.", CLERK_PORTAL_URL, PUBLICSEARCH_DEFAULT_URL)
    return PUBLICSEARCH_DEFAULT_URL


def is_publicsearch(url: str) -> bool:
    return "publicsearch.us" in (url or "").lower()


# --- Kofile / GovOS PublicSearch ------------------------------------------

def publicsearch_origin(url: str) -> str:
    match = re.match(r"(https?://[^/]+)", url or "")
    return match.group(1) if match else PUBLICSEARCH_DEFAULT_URL


def build_results_url(origin: str, code: str, start: datetime, end: datetime,
                      offset: int = 0, limit: int = PS_PAGE_LIMIT) -> str:
    params = (
        f"department={requests.utils.quote(PS_DEPARTMENT)}"
        f"&docTypes={requests.utils.quote(code)}"
        f"&recordedDateRange={start.strftime('%Y%m%d')}%2C{end.strftime('%Y%m%d')}"
        f"&searchType=advancedSearch&limit={limit}&offset={offset}"
    )
    return f"{origin}/results?{params}"


def doc_permalink(origin: str, doc_id: str, doc_num: str = "") -> str:
    if doc_id:
        return f"{origin}/doc/{doc_id}"
    if doc_num:
        return f"{origin}/results?department={PS_DEPARTMENT}&searchType=documentNumber&q={requests.utils.quote(doc_num)}"
    return origin


# Reads the PublicSearch results grid: header labels, row cells, row document id.
PS_ROWS_JS = """
() => {
  const txt = el => (el ? (el.innerText || el.textContent || '') : '').replace(/\\s+/g, ' ').trim();
  const body = document.body ? document.body.innerText : '';
  const empty = /No Results Found/i.test(body);
  const totalMatch = body.match(/(\\d[\\d,]*)\\s*-\\s*(\\d[\\d,]*)\\s*of\\s*([\\d,]+)/);
  const table = document.querySelector('table');
  const headers = table ? Array.from(table.querySelectorAll('th')).map(txt) : [];
  const rows = [];
  if (table) {
    for (const tr of Array.from(table.querySelectorAll('tbody tr'))) {
      const cells = Array.from(tr.querySelectorAll('td')).map(txt);
      const box = tr.querySelector('input[type=checkbox][id^="table-checkbox-"]');
      const docId = box ? box.id.replace('table-checkbox-', '') : '';
      if (cells.some(c => c)) rows.push({ cells, docId });
    }
  }
  return {
    empty,
    total: totalMatch ? parseInt(totalMatch[3].replace(/,/g, ''), 10) : null,
    shown: totalMatch ? parseInt(totalMatch[2].replace(/,/g, ''), 10) : null,
    headers, rows, url: location.href,
  };
}
"""

# Reads a PublicSearch document detail page (/doc/<id>).
PS_DETAIL_JS = """
() => {
  const txt = el => (el ? (el.innerText || el.textContent || '') : '').replace(/\\s+/g, ' ').trim();
  const body = document.body ? document.body.innerText : '';
  const field = label => {
    const re = new RegExp(label + '\\\\s*:?\\\\s*\\\\n([^\\\\n]*)', 'i');
    const m = body.match(re);
    return m ? m[1].trim() : '';
  };
  const parties = { grantors: [], grantees: [] };
  const lines = body.split('\\n').map(s => s.trim());
  for (let i = 0; i < lines.length; i++) {
    if (lines[i] === 'GRANTOR' && lines[i - 1]) parties.grantors.push(lines[i - 1]);
    if (lines[i] === 'GRANTEE' && lines[i - 1]) parties.grantees.push(lines[i - 1]);
  }
  return {
    consideration: field('Consideration'),
    docNumber: field('Document Number'),
    recordedDate: field('Recorded Date'),
    instrumentDate: field('Instrument Date'),
    legal: field('Legal Description'),
    grantors: parties.grantors,
    grantees: parties.grantees,
    url: location.href,
  };
}
"""


def ps_rows_to_records(payload: Dict[str, Any], code: str, origin: str,
                       start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Map PublicSearch grid rows to normalized records."""
    records: List[Dict[str, Any]] = []
    if not payload or payload.get("empty"):
        return records

    headers = [clean(h) for h in (payload.get("headers") or [])]
    mapping = map_headers(headers)
    for row in payload.get("rows") or []:
        try:
            cells = row.get("cells") or []
            values = {field: clean(cells[i]) for i, field in mapping.items() if i < len(cells)}
            if not values:  # known column order when headers are missing
                padded = cells + [""] * 10
                values = {
                    "owner": clean(padded[3]), "grantee": clean(padded[4]),
                    "doc_type": clean(padded[5]), "filed": clean(padded[6]),
                    "doc_num": clean(padded[7]), "legal": clean(padded[9]),
                }
            filed_dt = parse_date(values.get("filed"))
            if filed_dt and not (start.date() - timedelta(days=1)
                                 <= filed_dt.date() <= end.date() + timedelta(days=1)):
                continue
            doc_num = values.get("doc_num", "")
            owner = values.get("owner", "")
            if not doc_num and not owner:
                continue

            resolved_code, category, cat_label = classify(values.get("doc_type", ""), code)
            doc_id = clean(row.get("docId"))
            records.append({
                "doc_num": doc_num,
                "doc_type": values.get("doc_type", "") or cat_label,
                "filed": filed_dt.strftime("%Y-%m-%d") if filed_dt else iso_date(values.get("filed")),
                "cat": category,
                "cat_label": cat_label,
                "code": resolved_code,
                "owner": owner,
                "grantee": values.get("grantee", ""),
                "amount": parse_amount(values.get("amount")),
                "legal": values.get("legal", ""),
                "doc_id": doc_id,
                "_grantors": [owner] if owner else [],
                "_grantees": [values.get("grantee", "")] if values.get("grantee") else [],
                "clerk_url": doc_permalink(origin, doc_id, doc_num),
            })
        except Exception:  # noqa: BLE001 - one bad row never stops the page
            continue
    return records


async def ps_wait_for_results(page) -> Dict[str, Any]:
    """Wait for the results grid (or the 'no results' state) to render."""
    deadline = time.time() + (NAV_TIMEOUT_MS / 1000.0)
    payload: Dict[str, Any] = {}
    while time.time() < deadline:
        payload = await page.evaluate(PS_ROWS_JS) or {}
        if payload.get("rows") or payload.get("empty"):
            return payload
        await page.wait_for_timeout(700)
    return payload


async def ps_scrape_code(page, origin: str, code: str, start: datetime,
                         end: datetime) -> List[Dict[str, Any]]:
    """All filings of one document type in the window, paging by offset."""
    collected: List[Dict[str, Any]] = []
    offset = 0
    total: Optional[int] = None

    for page_index in range(MAX_PAGES_PER_TYPE):
        url = build_results_url(origin, code, start, end, offset=offset)
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        payload = await ps_wait_for_results(page)

        if payload.get("empty") and not payload.get("rows"):
            if page_index == 0:
                log.info("[%s] no filings in window (or the portal does not use this code)", code)
            break

        batch = ps_rows_to_records(payload, code, origin, start, end)
        collected.extend(batch)
        total = payload.get("total") or total
        log.info("[%s] offset %d: %d row(s)%s", code, offset, len(batch),
                 f" of {total} total" if total else "")

        if not batch or len(collected) >= MAX_RECORDS_PER_TYPE:
            break
        offset += PS_PAGE_LIMIT
        if total is not None and offset >= total:
            break
        await page.wait_for_timeout(400)

    return collected


async def ps_fill_details(page, origin: str, records: List[Dict[str, Any]]) -> None:
    """Visit /doc/<id> for each lead to pick up consideration + full party lists."""
    budget = min(len(records), MAX_DETAIL_FETCH)
    if budget <= 0:
        return
    log.info("Fetching detail pages for %d record(s)", budget)

    for index, record in enumerate(records[:budget]):
        doc_id = clean(record.get("doc_id"))
        if not doc_id:
            continue
        try:
            await page.goto(f"{origin}/doc/{doc_id}", wait_until="domcontentloaded",
                            timeout=NAV_TIMEOUT_MS)
            detail: Dict[str, Any] = {}
            deadline = time.time() + 20
            while time.time() < deadline:
                detail = await page.evaluate(PS_DETAIL_JS) or {}
                if detail.get("docNumber") or detail.get("grantors"):
                    break
                await page.wait_for_timeout(600)

            amount = parse_amount(detail.get("consideration"))
            if amount is not None:
                record["amount"] = amount
            grantors = list(dict.fromkeys(clean(g) for g in (detail.get("grantors") or []) if clean(g)))
            grantees = list(dict.fromkeys(clean(g) for g in (detail.get("grantees") or []) if clean(g)))
            if grantors:
                record["_grantors"] = grantors
                record["owner"] = grantors[0]
            if grantees:
                record["_grantees"] = grantees
                record["grantee"] = grantees[0]
            if clean(detail.get("legal")) and not clean(record.get("legal")):
                record["legal"] = clean(detail["legal"])
            if clean(detail.get("docNumber")) and not clean(record.get("doc_num")):
                record["doc_num"] = clean(detail["docNumber"])
        except Exception as exc:  # noqa: BLE001 - detail is a bonus, never required
            log.debug("Detail fetch failed for %s: %s", doc_id, exc)
        if index % 25 == 24:
            log.info("  …%d/%d detail pages", index + 1, budget)
        await page.wait_for_timeout(DETAIL_DELAY_MS)


async def ps_discover_doc_type_codes(page, origin: str, terms: Sequence[str],
                                     per_term: int = 6) -> Dict[str, str]:
    """
    Drive Advanced Search to learn the portal's real docTypes codes.

    Selecting a document type and submitting puts its internal code in the
    results URL (?docTypes=LP), so for each keyword we select each matching
    option in turn and read the code back. Run with --discover-types when the
    county changes its vocabulary, then set CLERK_DOC_TYPES from the output.
    """
    found: Dict[str, str] = {}

    list_js = """
    async (term) => {
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
      const input = document.getElementById('docTypes');
      if (!input) return null;
      input.focus();
      setter.call(input, term);
      input.dispatchEvent(new Event('input', { bubbles: true }));
      await sleep(1200);
      return Array.from(document.querySelectorAll('[id^="docTypes-item-"]'))
        .filter(n => n.getAttribute('aria-expanded') === null)
        .map(n => n.innerText.replace(/\s+/g, ' ').trim())
        .filter(Boolean);
    }
    """
    pick_js = """
    async ([term, label]) => {
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
      const input = document.getElementById('docTypes');
      if (!input) return 'no-field';
      input.focus();
      setter.call(input, term);
      input.dispatchEvent(new Event('input', { bubbles: true }));
      await sleep(1200);
      const opt = Array.from(document.querySelectorAll('[id^="docTypes-item-"], [role="option"]'))
        .find(n => n.innerText.replace(/\s+/g, ' ').trim() === label);
      if (!opt) return 'no-option';
      opt.click();
      await sleep(500);
      const btn = Array.from(document.querySelectorAll('button'))
        .find(b => /^search$/i.test((b.innerText || '').trim()));
      if (!btn) return 'no-button';
      btn.click();
      return 'ok';
    }
    """

    async def open_advanced() -> None:
        await page.goto(f"{origin}/search/advanced", wait_until="domcontentloaded",
                        timeout=NAV_TIMEOUT_MS)
        await page.wait_for_timeout(1800)

    for term in terms:
        keyword = term.strip().upper()
        if not keyword:
            continue
        try:
            await open_advanced()
            labels = await page.evaluate(list_js, keyword.split()[0]) or []
        except Exception as exc:  # noqa: BLE001
            log.debug("discover list failed for %s: %s", keyword, exc)
            continue

        labels = [l for l in labels if keyword in l.upper()] or \
                 [l for l in labels if keyword.split()[0] in l.upper()]
        if not labels:
            log.info("discover: %-32s (no matching document type)", keyword)
            continue

        for label in labels[:per_term]:
            if label in found:
                continue
            try:
                await open_advanced()
                status = await page.evaluate(pick_js, [keyword.split()[0], label])
                if status != "ok":
                    log.info("discover: %-32s %s", label, status)
                    continue
                await page.wait_for_timeout(2500)
                match = re.search(r"[?&]docTypes=([^&]+)", page.url)
                if match:
                    code = requests.utils.unquote(match.group(1))
                    found[label] = code
                    log.info("discover: %-32s -> %s", label, code)
            except Exception as exc:  # noqa: BLE001
                log.debug("discover failed for %s: %s", label, exc)
    return found


# --- Generic fallback for non-PublicSearch portals -------------------------

TABLE_EXTRACT_JS = """
() => {
  const tables = Array.from(document.querySelectorAll('table'));
  const scored = tables.map(t => ({t, n: t.querySelectorAll('tr').length}))
                       .filter(x => x.n >= 2)
                       .sort((a, b) => b.n - a.n);
  const grids = Array.from(document.querySelectorAll('[role="grid"], .k-grid, .rgMasterTable'));
  const table = scored.length ? scored[0].t : (grids.length ? grids[0] : null);
  if (!table) return null;
  const txt = el => (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
  const rows = Array.from(table.querySelectorAll('tr'));
  let headers = [];
  let bodyRows = rows;
  const headRow = table.querySelector('thead tr') ||
                  rows.find(r => r.querySelectorAll('th').length > 1);
  if (headRow) {
    headers = Array.from(headRow.querySelectorAll('th,td')).map(txt);
    bodyRows = rows.filter(r => r !== headRow);
  }
  const out = [];
  for (const r of bodyRows) {
    const cells = Array.from(r.querySelectorAll('td'));
    if (!cells.length) continue;
    const links = Array.from(r.querySelectorAll('a'))
      .map(a => ({ href: a.href || '', text: txt(a) }))
      .filter(a => a.href);
    out.push({ cells: cells.map(txt), links });
  }
  return { headers, rows: out, url: location.href };
}
"""


def map_headers(headers: Sequence[str]) -> Dict[int, str]:
    """Column index -> canonical field name."""
    mapping: Dict[int, str] = {}
    taken: Set[str] = set()
    for index, header in enumerate(headers):
        text = clean(header).lower()
        if not text:
            continue
        for field, pattern in HEADER_MAP:
            if field in taken:
                continue
            if re.search(pattern, text, re.I):
                mapping[index] = field
                taken.add(field)
                break
    return mapping


def rows_to_records(payload: Dict[str, Any], requested_code: str, base_url: str,
                    start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Turn one generic results table into normalized record dicts."""
    records: List[Dict[str, Any]] = []
    if not payload:
        return records
    mapping = map_headers(payload.get("headers") or [])
    page_url = payload.get("url") or base_url

    for row in payload.get("rows") or []:
        try:
            cells = row.get("cells") or []
            if not cells or all(not clean(c) for c in cells):
                continue
            values = {field: clean(cells[i]) for i, field in mapping.items() if i < len(cells)}
            if not values:
                joined = " | ".join(clean(c) for c in cells)
                match = re.search(r"\b(\d{4}-?\d{6,})\b|\b(\d{8,})\b", joined)
                values = {
                    "doc_num": match.group(0) if match else "",
                    "filed": next((clean(c) for c in cells if parse_date(c)), ""),
                    "doc_type": next((clean(c) for c in cells
                                      if re.search(r"[A-Za-z]{3}", clean(c))), ""),
                }

            filed_dt = parse_date(values.get("filed"))
            if filed_dt and not (start.date() - timedelta(days=1)
                                 <= filed_dt.date() <= end.date() + timedelta(days=1)):
                continue
            if not values.get("doc_num") and not values.get("owner"):
                continue

            code, category, cat_label = classify(values.get("doc_type", ""), requested_code)
            href = next((l.get("href", "") for l in (row.get("links") or []) if l.get("href")), "")
            doc_num = values.get("doc_num", "")
            records.append({
                "doc_num": doc_num,
                "doc_type": values.get("doc_type", "") or cat_label,
                "filed": filed_dt.strftime("%Y-%m-%d") if filed_dt else iso_date(values.get("filed")),
                "cat": category,
                "cat_label": cat_label,
                "code": code,
                "owner": values.get("owner", ""),
                "grantee": values.get("grantee", ""),
                "amount": parse_amount(values.get("amount")),
                "legal": values.get("legal", ""),
                "doc_id": "",
                "_grantors": [values.get("owner", "")] if values.get("owner") else [],
                "_grantees": [values.get("grantee", "")] if values.get("grantee") else [],
                "clerk_url": href if href.startswith("http") else page_url,
            })
        except Exception:  # noqa: BLE001
            continue
    return records


async def _first_visible(page, selectors: Sequence[str]):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() and await locator.is_visible():
                return locator
        except Exception:  # noqa: BLE001
            continue
    return None


async def dismiss_disclaimer(page) -> None:
    """Click through 'I accept' / 'Public Login' style gates. Safe if absent."""
    for _ in range(3):
        clicked = False
        try:
            candidates = page.locator("button, input[type=submit], input[type=button], a")
            count = min(await candidates.count(), 60)
            for i in range(count):
                node = candidates.nth(i)
                try:
                    if not await node.is_visible():
                        continue
                    label = clean(await node.inner_text() or "") or clean(await node.get_attribute("value") or "")
                    if label and DISCLAIMER_HINTS.match(label):
                        log.info("Clicking gate button: %s", label)
                        await node.click(timeout=10000)
                        await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT_MS)
                        await page.wait_for_timeout(1500)
                        clicked = True
                        break
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            return
        if not clicked:
            return


async def find_search_form_fields(page) -> Dict[str, Any]:
    """Locate date-from / date-to / doc-type / submit controls heuristically."""
    fields: Dict[str, Any] = {}
    try:
        inputs = page.locator("input, select, textarea")
        count = min(await inputs.count(), 200)
        for i in range(count):
            node = inputs.nth(i)
            try:
                if not await node.is_visible():
                    continue
                attrs = " ".join(filter(None, [
                    await node.get_attribute("name") or "",
                    await node.get_attribute("id") or "",
                    await node.get_attribute("placeholder") or "",
                    await node.get_attribute("aria-label") or "",
                    await node.get_attribute("title") or "",
                ]))
                tag = (await node.evaluate("el => el.tagName")).lower()
                if "date_from" not in fields and DATE_FROM_HINTS.search(attrs):
                    fields["date_from"] = node
                elif "date_to" not in fields and DATE_TO_HINTS.search(attrs):
                    fields["date_to"] = node
                elif "doc_type" not in fields and DOCTYPE_HINTS.search(attrs):
                    fields["doc_type"] = node
                    fields["doc_type_tag"] = tag
            except Exception:  # noqa: BLE001
                continue
    except Exception as exc:  # noqa: BLE001
        log.debug("Field discovery failed: %s", exc)

    fields["submit"] = await _first_visible(page, [
        "input[type=submit][value*='Search' i]",
        "button:has-text('Search')",
        "input[type=button][value*='Search' i]",
        "a:has-text('Search')",
        "button[type=submit]",
        "input[type=submit]",
    ])
    return fields


async def set_doc_type(page, node, tag: str, code: str, label: str) -> bool:
    """Fill the doc-type control whether it's a <select>, a combo, or a text box."""
    try:
        if tag == "select":
            options = await node.evaluate(
                "el => Array.from(el.options).map(o => ({v: o.value, t: o.text}))")
            wanted = None
            for option in options:
                value = clean(option.get("v")).upper()
                text = clean(option.get("t")).upper()
                if value == code.upper() or text.startswith(code.upper() + " ") or text == code.upper():
                    wanted = option
                    break
            if wanted is None:
                for option in options:
                    if label.upper().split("(")[0].strip() in clean(option.get("t")).upper():
                        wanted = option
                        break
            if wanted is None:
                return False
            await node.select_option(value=wanted["v"])
            return True
        await node.fill("")
        await node.type(code, delay=25)
        await page.wait_for_timeout(600)
        try:
            suggestion = page.locator("li[role=option], .ui-menu-item, .autocomplete-item").first
            if await suggestion.count() and await suggestion.is_visible():
                await suggestion.click(timeout=3000)
        except Exception:  # noqa: BLE001
            pass
        return True
    except Exception as exc:  # noqa: BLE001
        log.debug("Could not set doc type %s: %s", code, exc)
        return False


async def fill_date(node, value: str) -> None:
    try:
        await node.click(timeout=5000)
        await node.fill("")
        await node.type(value, delay=20)
    except Exception:  # noqa: BLE001
        try:
            await node.fill(value)
        except Exception:  # noqa: BLE001
            pass


async def generic_scrape_code(page, search_url: str, code: str, start: datetime,
                              end: datetime) -> List[Dict[str, Any]]:
    """Fallback: fill whatever search form the portal presents, then page through."""
    _, label = DOC_TYPES.get(code, ("other", code))
    collected: List[Dict[str, Any]] = []

    await page.goto(search_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await dismiss_disclaimer(page)
    await page.wait_for_timeout(1000)

    fields = await find_search_form_fields(page)
    if not fields.get("date_from") and not fields.get("doc_type"):
        log.warning("[%s] No recognizable search form at %s", code, search_url)
        return collected

    if fields.get("date_from"):
        await fill_date(fields["date_from"], start.strftime("%m/%d/%Y"))
    if fields.get("date_to"):
        await fill_date(fields["date_to"], end.strftime("%m/%d/%Y"))
    if fields.get("doc_type") is not None:
        if not await set_doc_type(page, fields["doc_type"], fields.get("doc_type_tag", "input"), code, label):
            log.info("[%s] No matching doc-type option; searching by date only", code)

    submitted = False
    if fields.get("submit") is not None:
        try:
            await fields["submit"].click(timeout=15000)
            submitted = True
        except Exception as exc:  # noqa: BLE001
            log.debug("[%s] Submit click failed: %s", code, exc)
    if not submitted:
        try:
            await page.keyboard.press("Enter")
        except Exception:  # noqa: BLE001
            pass
    try:
        await page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)
    except Exception:  # noqa: BLE001
        await page.wait_for_timeout(3000)

    for page_number in range(1, MAX_PAGES_PER_TYPE + 1):
        try:
            payload = await page.evaluate(TABLE_EXTRACT_JS)
        except Exception as exc:  # noqa: BLE001
            log.debug("[%s] Table extraction failed on page %d: %s", code, page_number, exc)
            break
        batch = rows_to_records(payload, code, page.url, start, end)
        collected.extend(batch)
        log.info("[%s] page %d: %d row(s) (total %d)", code, page_number, len(batch), len(collected))
        if not batch or len(collected) >= MAX_RECORDS_PER_TYPE:
            break

        moved = False
        try:
            links = page.locator("a, button, input[type=button], input[type=submit]")
            count = min(await links.count(), 80)
            for i in range(count):
                node = links.nth(i)
                try:
                    if not await node.is_visible():
                        continue
                    text = clean(await node.inner_text() or "") or clean(await node.get_attribute("value") or "")
                    if text and NEXT_PAGE_HINTS.match(text):
                        classes = (await node.get_attribute("class") or "").lower()
                        if await node.get_attribute("disabled") is not None or "disabled" in classes:
                            break
                        await node.click(timeout=10000)
                        await page.wait_for_timeout(2500)
                        moved = True
                        break
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            pass
        if not moved:
            break

    return collected


# --- Driver ----------------------------------------------------------------

async def scrape_clerk(search_url: str, start: datetime, end: datetime,
                       codes: Sequence[str], with_details: bool = True,
                       discover_terms: Sequence[str] = ()) -> List[Dict[str, Any]]:
    """Drive the clerk portal for every requested document type."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("playwright is not installed — no clerk records will be collected "
                  "(pip install playwright && python -m playwright install chromium)")
        return []

    records: List[Dict[str, Any]] = []
    origin = publicsearch_origin(search_url)
    use_publicsearch = is_publicsearch(search_url)
    log.info("Clerk portal mode: %s (%s)",
             "PublicSearch" if use_publicsearch else "generic form", origin)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1600, "height": 1000},
            ignore_https_errors=True,
        )
        context.set_default_timeout(NAV_TIMEOUT_MS)
        page = await context.new_page()

        try:
            if discover_terms:
                mapping = await ps_discover_doc_type_codes(page, origin, discover_terms)
                print(json.dumps(mapping, indent=2))
                return []

            for code in codes:
                if use_publicsearch:
                    batch = await aretry(lambda c=code: ps_scrape_code(page, origin, c, start, end),
                                         label=f"clerk search {code}")
                else:
                    batch = await aretry(lambda c=code: generic_scrape_code(page, search_url, c, start, end),
                                         label=f"clerk search {code}")
                if batch:
                    records.extend(batch)
                await asyncio.sleep(0.8)

            records = dedupe(filter_notices(records))
            if use_publicsearch and with_details and records:
                await aretry(lambda: ps_fill_details(page, origin, records),
                             label="detail pages")
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:  # noqa: BLE001
                pass

    log.info("Clerk portal returned %d record(s)", len(records))
    return records


# ---------------------------------------------------------------------------
# County foreclosure notices (apps2.collincountytx.gov/ForeclosureNotices)
# ---------------------------------------------------------------------------
#
# Notices of substitute trustee's sale are not in the clerk's real-property
# index — the county runs a separate Blazor app for them, linked from the clerk
# page as "Foreclosures". Each notice carries the property address, city, sale
# date, filed date and property type, but no owner name; the owner is recovered
# afterwards from the CollinCAD address index.

MAX_FORECLOSURE_PAGES = int(os.getenv("MAX_FORECLOSURE_PAGES", "40"))

# Types the county publishes for non-residential/other parcels are kept too —
# filtering happens on the lead side, not here.
FN_SET_DATES_JS = """
async ([startText, endText]) => {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  const labels = Array.from(document.querySelectorAll('.mud-input-label, label'));
  const findInput = text => {
    const label = labels.find(l => l.innerText.trim().toLowerCase() === text);
    if (!label) return null;
    const wrap = label.closest('.mud-input-control') || label.parentElement;
    return wrap ? wrap.querySelector('input') : null;
  };
  const start = findInput('filed date start');
  const end = findInput('filed date end');
  if (!start || !end) return 'no-date-fields';
  for (const [el, value] of [[start, startText], [end, endText]]) {
    el.focus();
    setter.call(el, value);
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.blur();
    await sleep(900);
  }
  return 'ok';
}
"""

FN_ROWS_JS = """
() => {
  const rows = Array.from(document.querySelectorAll('tr.mud-table-row'));
  const field = (text, label) => {
    const m = text.match(new RegExp(label + '\\s*:?\\s*([^\\n]*)', 'i'));
    return m ? m[1].trim() : '';
  };
  const out = rows.map(row => {
    const header = row.querySelector('.list-header');
    const headerText = header ? (header.innerText || '').trim() : '';
    const text = (row.innerText || '').replace(/\u00a0/g, ' ');
    return {
      header: headerText,
      city: field(text, 'City'),
      saleDate: field(text, 'Sale Date'),
      fileDate: field(text, 'File Date'),
      propertyType: field(text, 'Property Type'),
    };
  }).filter(r => r.header);
  const pageButtons = Array.from(document.querySelectorAll('button'))
    .map(b => b.getAttribute('aria-label') || '')
    .filter(a => a.startsWith('Page ') || a.startsWith('Current page'));
  return { rows: out, pages: pageButtons.length, url: location.href };
}
"""

FN_NEXT_JS = """
() => {
  const next = Array.from(document.querySelectorAll('button'))
    .find(b => (b.getAttribute('aria-label') || '') === 'Next page');
  if (!next || next.disabled || next.classList.contains('mud-disabled')) return false;
  next.click();
  return true;
}
"""


CITY_STATE_ZIP_RE = re.compile(
    r"^(?P<city>[A-Za-z .'-]+),\s*(?P<state>[A-Z]{2})\s*(?P<zip>\d{5}(?:-\d{4})?)?$")
STATE_ZIP_TAIL_RE = re.compile(r",?\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)\s*$")


def parse_notice_address(header: str, city_hint: str = "") -> Dict[str, str]:
    """
    '1000 MANCHESTER DR WYLIE, TX 75098' -> street / city / state / zip.

    The notice app renders the address as one blob whose city is not reliably
    delimited, so the row's own "City:" value is used to trim the city off the
    end of the street line when it is available.
    """
    raw = str(header or "").replace("\r", "\n")
    lines = [clean(line) for line in raw.split("\n") if clean(line)]
    if not lines:
        return {"prop_address": "", "prop_city": clean(city_hint).upper(),
                "prop_state": "", "prop_zip": ""}

    # Two-line form: street, then "CITY, ST ZIP".
    for index in range(len(lines) - 1, 0, -1):
        match = CITY_STATE_ZIP_RE.match(lines[index])
        if match:
            return {
                "prop_address": clean(" ".join(lines[:index])),
                "prop_city": clean(match.group("city")).upper(),
                "prop_state": match.group("state"),
                "prop_zip": clean(match.group("zip")),
            }

    text = " ".join(lines)
    state, postal = "TX", ""
    tail = STATE_ZIP_TAIL_RE.search(text)
    if tail:
        state, postal = tail.group("state"), tail.group("zip")
        text = text[:tail.start()].rstrip(" ,")

    city = clean(city_hint).upper()
    upper = text.upper()
    if city and upper.endswith(city):
        text = text[:len(text) - len(city)].rstrip(" ,")
    elif "," in text:
        head, _, tail_city = text.rpartition(",")
        text, city = head, clean(tail_city).upper()

    return {"prop_address": clean(text), "prop_city": city,
            "prop_state": state, "prop_zip": clean(postal)}


def notice_to_record(row: Dict[str, Any], source_url: str,
                     start: datetime, end: datetime) -> Optional[Dict[str, Any]]:
    try:
        filed_dt = parse_date(row.get("fileDate"))
        if filed_dt and not (start.date() - timedelta(days=1)
                             <= filed_dt.date() <= end.date() + timedelta(days=1)):
            return None
        address = parse_notice_address(row.get("header", ""), clean(row.get("city")))
        street = address["prop_address"]
        # The list interleaves group headers such as "FILED 9/1/2026 - OCTOBER SALE".
        if not street or re.match(r"^(FILED|SALE|NOTICE|PAGE)\b", street, re.I) \
                or not re.match(r"^\d|^[NSEW]\s+\d", street):
            return None
        if not address["prop_city"]:
            address["prop_city"] = clean(row.get("city")).upper()

        sale_date = clean(row.get("saleDate"))
        property_type = clean(row.get("propertyType"))
        legal = " · ".join(filter(None, [f"Sale date: {sale_date}" if sale_date else "",
                                         property_type]))
        record = {
            "doc_num": "",
            "doc_type": "Notice of Foreclosure Sale",
            "filed": filed_dt.strftime("%Y-%m-%d") if filed_dt else iso_date(row.get("fileDate")),
            "cat": "foreclosure",
            "cat_label": "Notice of Foreclosure Sale",
            "code": "NOFC",
            "owner": "",
            "grantee": "",
            "amount": None,
            "legal": legal,
            "doc_id": "",
            "_grantors": [],
            "_grantees": [],
            "clerk_url": source_url,
        }
        record.update(address)
        return record
    except Exception:  # noqa: BLE001 - one malformed notice never stops the page
        return None


async def apply_notice_date_filter(page, start: datetime, end: datetime) -> bool:
    """
    Try to narrow the notice list with the app's "Filed Date Start/End" filters.

    MudBlazor binds these fields through its own date picker rather than the raw
    input, and clicking one opens an overlay that swallows later clicks, so the
    value is filled (not clicked) and the picker dismissed with Escape. The app
    does not always honour it, so success is confirmed by checking that the rows
    on screen actually fall inside the window; when it does not, the caller pages
    the whole list and filters locally instead.
    """
    values = (start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y"))
    try:
        for label, value in zip(("Filed Date Start", "Filed Date End"), values):
            field = page.locator(".mud-input-control", has_text=label).last.locator("input").first
            await field.fill(value, timeout=15000)
            await field.press("Enter")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(900)
    except Exception as exc:  # noqa: BLE001 - fall through to the scripted path
        log.debug("Typed date filter failed: %s", exc)
        status = await page.evaluate(FN_SET_DATES_JS, list(values))
        if status != "ok":
            log.info("Foreclosure date filter unavailable (%s)", status)
            return False
    await page.wait_for_timeout(2500)

    payload = await page.evaluate(FN_ROWS_JS) or {}
    rows = payload.get("rows") or []
    if not rows:
        return False
    in_window = 0
    for row in rows:
        filed = parse_date(row.get("fileDate"))
        if filed and start.date() - timedelta(days=1) <= filed.date() <= end.date() + timedelta(days=1):
            in_window += 1
    if in_window == len(rows):
        log.info("Foreclosure date filter applied (%d row(s) on the first page)", len(rows))
        return True
    return False


async def scrape_foreclosure_notices(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Every notice of foreclosure sale filed in the window, with its address."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("playwright is not installed — skipping foreclosure notices")
        return []

    records: List[Dict[str, Any]] = []
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(user_agent=USER_AGENT,
                                            viewport={"width": 1600, "height": 1000},
                                            ignore_https_errors=True)
        context.set_default_timeout(NAV_TIMEOUT_MS)
        page = await context.new_page()
        try:
            await page.goto(FORECLOSURE_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            try:
                await page.wait_for_selector("tr.mud-table-row", timeout=NAV_TIMEOUT_MS)
            except Exception:  # noqa: BLE001
                log.warning("Foreclosure notices table never rendered")
                return []

            await page.wait_for_timeout(3000)  # let the Blazor circuit connect
            if not await apply_notice_date_filter(page, start, end):
                log.info("Foreclosure notices: paging the full list and filtering by "
                         "filed date locally")

            seen: Set[str] = set()
            for page_number in range(1, MAX_FORECLOSURE_PAGES + 1):
                payload = await page.evaluate(FN_ROWS_JS) or {}
                rows = payload.get("rows") or []
                fresh = 0
                for row in rows:
                    record = notice_to_record(row, FORECLOSURE_URL, start, end)
                    if not record:
                        continue
                    key = f"{normalize_address(record['prop_address'])}|{record['filed']}"
                    if key in seen:
                        continue
                    seen.add(key)
                    records.append(record)
                    fresh += 1
                log.info("[NOFC] page %d: %d notice(s) in window (total %d)",
                         page_number, fresh, len(records))

                if not await page.evaluate(FN_NEXT_JS):
                    break
                await page.wait_for_timeout(1600)
        except Exception as exc:  # noqa: BLE001 - never fail the whole run
            log.error("Foreclosure notice scrape failed: %s", exc)
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:  # noqa: BLE001
                pass

    log.info("Foreclosure notices: %d record(s)", len(records))
    return records


# ---------------------------------------------------------------------------
# Flags + seller score
# ---------------------------------------------------------------------------

FLAG_LIS_PENDENS = "Lis pendens"
FLAG_PRE_FORECLOSURE = "Pre-foreclosure"
FLAG_JUDGMENT = "Judgment lien"
FLAG_TAX_LIEN = "Tax lien"
FLAG_MECHANIC = "Mechanic lien"
FLAG_PROBATE = "Probate / estate"
FLAG_CORP = "LLC / corp owner"
FLAG_NEW = "New this week"

CATEGORY_FLAGS: Dict[str, str] = {
    "lis_pendens": FLAG_LIS_PENDENS,
    "foreclosure": FLAG_PRE_FORECLOSURE,
    "tax_deed": FLAG_PRE_FORECLOSURE,
    "judgment": FLAG_JUDGMENT,
    "tax_lien": FLAG_TAX_LIEN,
    "medicaid_lien": FLAG_TAX_LIEN,
    "mechanic_lien": FLAG_MECHANIC,
    "probate": FLAG_PROBATE,
}


def compute_flags(record: Dict[str, Any], now: datetime) -> List[str]:
    flags: List[str] = []
    category = record.get("cat", "")
    mapped = CATEGORY_FLAGS.get(category)
    if mapped:
        flags.append(mapped)
    if record.get("code") in ("ML", "LNMECH") and FLAG_MECHANIC not in flags:
        flags.append(FLAG_MECHANIC)
    if CORP_PATTERN.search(normalize_name(record.get("owner"))):
        flags.append(FLAG_CORP)
    filed = parse_date(record.get("filed"))
    if filed and (now.date() - filed.date()).days <= 7:
        flags.append(FLAG_NEW)
    # Preserve order, drop duplicates.
    return list(dict.fromkeys(flags))


def score_record(record: Dict[str, Any], owner_flag_pool: Set[str]) -> int:
    """Base 30, +10/flag, +20 LP+foreclosure combo, amount and freshness bonuses."""
    score = 30
    flags = record.get("flags") or []
    score += 10 * len(flags)

    combo_pool = set(flags) | owner_flag_pool
    if FLAG_LIS_PENDENS in combo_pool and FLAG_PRE_FORECLOSURE in combo_pool:
        score += 20

    amount = record.get("amount")
    if isinstance(amount, (int, float)):
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    if FLAG_NEW in flags:
        score += 5
    if clean(record.get("prop_address")) or clean(record.get("mail_address")):
        score += 5

    return max(0, min(100, score))


def filter_notices(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep only the generic NOTICE filings that are actually foreclosure related,
    and promote those to the pre-foreclosure category. Everything else passes
    through untouched.
    """
    kept: List[Dict[str, Any]] = []
    dropped = 0
    for record in records:
        try:
            if record.get("cat") != "notice":
                kept.append(record)
                continue
            blob = " ".join(str(record.get(field, "")) for field in
                            ("owner", "grantee", "legal", "doc_type"))
            for extra in ("_grantors", "_grantees"):
                blob += " " + " ".join(record.get(extra) or [])
            if FORECLOSURE_PARTY_PATTERN.search(blob):
                record["cat"] = "foreclosure"
                record["cat_label"] = "Notice of Trustee's Sale"
                kept.append(record)
            else:
                dropped += 1
        except Exception:  # noqa: BLE001
            kept.append(record)
    if dropped:
        log.info("Dropped %d unrelated NOTICE filing(s)", dropped)
    return kept


def dedupe(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per (doc number | owner+date+type). Keeps the richest copy."""
    best: Dict[str, Dict[str, Any]] = {}
    for record in records:
        try:
            doc_num = clean(record.get("doc_num")).upper()
            key = doc_num or "|".join([
                normalize_name(record.get("owner")),
                normalize_address(record.get("prop_address")),
                clean(record.get("filed")),
                clean(record.get("code")),
            ])
            if not key.strip("|"):
                continue
            existing = best.get(key)
            if existing is None:
                best[key] = record
                continue
            richness = lambda r: sum(1 for v in r.values() if clean(v))  # noqa: E731
            if richness(record) > richness(existing):
                best[key] = record
        except Exception:  # noqa: BLE001
            continue
    return list(best.values())


def resolve_lead_party(record: Dict[str, Any],
                       parcel_index: Optional[ParcelIndex]) -> Tuple[str, str, Dict[str, str]]:
    """
    Decide which party on a filing is the motivated seller.

    Filings name two sides and neither column is reliably the owner: a bank, HOA,
    contractor or taxing authority files *against* the homeowner (owner is the
    grantee), while some lis pendens and probate filings list the homeowner first.
    Candidates are therefore ranked

        1. people before companies, companies before banks/HOAs/agencies, then
        2. the grantee side before the grantor side, then
        3. whoever actually owns a Collin County parcel — but only as a tiebreak
           inside one rank, so a judgment creditor who happens to own land never
           outranks the debtor the judgment was filed against.

    Returns (owner, counterparty, parcel).
    """
    grantors = [clean(n) for n in (record.get("_grantors") or []) if clean(n)]
    grantees = [clean(n) for n in (record.get("_grantees") or []) if clean(n)]
    if not grantors and clean(record.get("owner")):
        grantors = [clean(record["owner"])]
    if not grantees and clean(record.get("grantee")):
        grantees = [clean(record["grantee"])]

    def counterparty(owner_name: str, preferred: List[str], fallback: List[str]) -> str:
        """The other side of the filing — never the owner echoed back."""
        owner_key = normalize_name(owner_name)
        pools = [preferred, fallback]
        for pool in pools:  # an institution is the informative counterparty
            for name in pool:
                if normalize_name(name) != owner_key and is_institution(name):
                    return name
        for pool in pools:
            for name in pool:
                if normalize_name(name) != owner_key:
                    return name
        return ""

    def tier(name: str) -> int:
        """0 = a person, 1 = a company that is not an obvious filer, 2 = a filer."""
        if is_institution(name):
            return 2
        return 1 if CORP_PATTERN.search(normalize_name(name)) else 0

    # Rank: people before companies before banks/HOAs/agencies, and grantee
    # before grantor — on a judgment, lien, tax lien or lis pendens the creditor
    # files against the owner, so the owner is usually the grantee. Owning a
    # Collin County parcel only breaks ties *within* a rank; it must not promote
    # a judgment creditor over the debtor just because the creditor owns property.
    candidates: List[Tuple[int, int, int, str, str]] = []
    for side_rank, (names, side) in enumerate(((grantees, "grantee"), (grantors, "grantor"))):
        for position, name in enumerate(names):
            candidates.append((tier(name), side_rank, position, name, side))
    candidates.sort(key=lambda c: (c[0], c[1], c[2]))

    def sides_for(side: str) -> Tuple[List[str], List[str]]:
        return (grantees, grantors) if side == "grantor" else (grantors, grantees)

    def choose(group: List[Tuple[int, int, int, str, str]]):
        """Within one rank, prefer a party that actually owns Collin property."""
        for _, _, _, name, side in group:
            parcel = lookup_parcel(parcel_index, name)
            if parcel:
                other, rest = sides_for(side)
                return name, counterparty(name, other, rest), parcel
        _, _, _, name, side = group[0]
        other, rest = sides_for(side)
        return name, counterparty(name, other, rest), {}

    # Walk (tier, side) groups in order: person-grantee, person-grantor,
    # company-grantee, company-grantor. Parcel ownership only breaks ties inside
    # a group, so the debtor still wins over a creditor who happens to own land.
    for rank in (0, 1):
        for side_rank in (0, 1):
            group = [c for c in candidates if c[0] == rank and c[1] == side_rank]
            if group:
                return choose(group)

    # Everyone left is a filer-type entity (LLC-owned property is still a lead).
    if candidates:
        return choose(candidates)
    return (grantors[0] if grantors else clean(record.get("owner"))), \
           (grantees[0] if grantees else clean(record.get("grantee"))), {}


def enrich_and_score(raw_records: Sequence[Dict[str, Any]],
                     parcel_index: Optional[ParcelIndex]) -> List[Dict[str, Any]]:
    now = utcnow()
    records: List[Dict[str, Any]] = []

    for raw in raw_records:
        try:
            record = dict(raw)
            record.setdefault("owner", "")
            owner, counterparty, parcel = resolve_lead_party(record, parcel_index)
            record["owner"] = tidy_name(owner) or tidy_name(record.get("owner"))
            record["grantee"] = tidy_name(counterparty) or tidy_name(record.get("grantee"))

            # Foreclosure notices arrive with an address but no owner: look the
            # property up in the parcel index and take the owner from there.
            if not parcel and parcel_index and clean(record.get("prop_address")):
                by_address = parcel_index.address_lookup(record.get("prop_address"),
                                                         record.get("prop_zip"))
                if by_address:
                    parcel = by_address
                    if not clean(record.get("owner")):
                        record["owner"] = tidy_name(by_address.get("owner"))

            record["prop_address"] = parcel.get("prop_address") or clean(record.get("prop_address"))
            record["prop_city"] = parcel.get("prop_city") or clean(record.get("prop_city"))
            record["prop_state"] = (parcel.get("prop_state") or clean(record.get("prop_state"))
                                    or ("TX" if record.get("prop_address") else ""))
            record["prop_zip"] = parcel.get("prop_zip") or clean(record.get("prop_zip"))
            record["mail_address"] = parcel.get("mail_address", "")
            record["mail_city"] = parcel.get("mail_city", "")
            record["mail_state"] = parcel.get("mail_state", "") if parcel else ""
            record["mail_zip"] = parcel.get("mail_zip", "")
            record["flags"] = compute_flags(record, now)
            records.append(record)
        except Exception:  # noqa: BLE001 - never let one record kill the batch
            continue

    # Owner-level flag pool so an LP on one filing and a foreclosure on another
    # for the same owner still earns the combo bonus.
    owner_flags: Dict[str, Set[str]] = {}
    for record in records:
        key = normalize_name(record.get("owner"))
        if key:
            owner_flags.setdefault(key, set()).update(record.get("flags") or [])

    for record in records:
        try:
            pool = owner_flags.get(normalize_name(record.get("owner")), set())
            record["score"] = score_record(record, pool)
            record.pop("code", None)
        except Exception:  # noqa: BLE001
            record["score"] = 30

    # Highest score first, then most recently filed.
    records.sort(key=lambda r: (-int(r.get("score") or 0), _neg_filed_key(r)))
    return records


def _neg_filed_key(record: Dict[str, Any]) -> str:
    """Sort key that puts newer filings first within an equal score."""
    filed = parse_date(record.get("filed"))
    return "" if filed is None else str(9999 - filed.year) + filed.strftime("%m%d")


FIELD_ORDER = [
    "doc_num", "doc_type", "filed", "cat", "cat_label", "owner", "grantee", "amount",
    "legal", "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip", "clerk_url", "flags", "score",
]


def build_payload(records: Sequence[Dict[str, Any]], start: datetime, end: datetime) -> Dict[str, Any]:
    shaped: List[Dict[str, Any]] = []
    for record in records:
        shaped.append({field: record.get(field, "" if field != "amount" else None)
                       for field in FIELD_ORDER})
    with_address = sum(1 for r in shaped if clean(r.get("prop_address")) or clean(r.get("mail_address")))
    return {
        "fetched_at": utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": SOURCE_LABEL,
        "date_range": f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
        "total": len(shaped),
        "with_address": with_address,
        "records": shaped,
    }


def write_outputs(payload: Dict[str, Any]) -> List[Path]:
    targets = [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]
    written: List[Path] = []
    for target in targets:
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            written.append(target)
            log.info("Wrote %s (%d records)", target.relative_to(ROOT), payload["total"])
        except Exception as exc:  # noqa: BLE001
            log.error("Could not write %s: %s", target, exc)
    return written


# ---------------------------------------------------------------------------
# GoHighLevel (GHL) CSV export
# ---------------------------------------------------------------------------

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State",
    "Mailing Zip", "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
]


def export_ghl_csv(payload: Dict[str, Any], path: Path) -> Optional[Path]:
    """Write a GoHighLevel-importable contact CSV. Returns the path, or None."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=GHL_COLUMNS)
            writer.writeheader()
            for record in payload.get("records", []):
                try:
                    first, last = split_person_name(record.get("owner"))
                    amount = record.get("amount")
                    writer.writerow({
                        "First Name": first,
                        "Last Name": last or clean(record.get("owner")),
                        "Mailing Address": clean(record.get("mail_address")),
                        "Mailing City": clean(record.get("mail_city")),
                        "Mailing State": clean(record.get("mail_state")),
                        "Mailing Zip": clean(record.get("mail_zip")),
                        "Property Address": clean(record.get("prop_address")),
                        "Property City": clean(record.get("prop_city")),
                        "Property State": clean(record.get("prop_state")),
                        "Property Zip": clean(record.get("prop_zip")),
                        "Lead Type": clean(record.get("cat_label")),
                        "Document Type": clean(record.get("doc_type")),
                        "Date Filed": clean(record.get("filed")),
                        "Document Number": clean(record.get("doc_num")),
                        "Amount/Debt Owed": f"{amount:.2f}" if isinstance(amount, (int, float)) else "",
                        "Seller Score": record.get("score", ""),
                        "Motivated Seller Flags": "; ".join(record.get("flags") or []),
                        "Source": payload.get("source", SOURCE_LABEL),
                        "Public Records URL": clean(record.get("clerk_url")),
                    })
                except Exception:  # noqa: BLE001 - skip one bad contact, keep the file
                    continue
        log.info("Wrote %s", path)
        return path
    except Exception as exc:  # noqa: BLE001
        log.error("GHL export failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collin County motivated seller lead scraper")
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                        help=f"days of filings to pull (default {LOOKBACK_DAYS})")
    parser.add_argument("--no-parcels", action="store_true",
                        help="skip the CollinCAD bulk download / address enrichment")
    parser.add_argument("--no-clerk", action="store_true",
                        help="skip all scraping and re-score/re-join data/records.json")
    parser.add_argument("--no-foreclosures", action="store_true",
                        help="skip the county foreclosure-notice app")
    parser.add_argument("--types", default="",
                        help="comma-separated doc-type codes (default: all configured)")
    parser.add_argument("--export-csv", default="",
                        help="extra path for the GHL CSV export")
    parser.add_argument("--no-details", action="store_true",
                        help="skip per-document detail pages (faster, no consideration amounts)")
    parser.add_argument("--discover-types", default="",
                        help="comma-separated document-type LABELS to look up the portal's "
                             "internal codes for (e.g. 'LIS PENDENS,PROBATE'), then exit")
    parser.add_argument("--debug", action="store_true", help="verbose logging, headful browser")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    global HEADLESS
    if args.debug:
        HEADLESS = False

    start, end = date_range(max(1, args.lookback))
    configured = os.getenv("CLERK_DOC_TYPES", "")
    codes = ([c.strip().upper() for c in args.types.split(",") if c.strip()]
             or [c.strip().upper() for c in configured.split(",") if c.strip()]
             or list(DEFAULT_QUERY_CODES))
    unknown = [c for c in codes if c not in DOC_TYPES]
    if unknown:
        log.info("Doc-type code(s) with no local label: %s (kept; classified from the "
                 "portal's own description text)", ", ".join(unknown))

    session = make_session()

    if args.discover_types:
        terms = [t.strip() for t in args.discover_types.split(",") if t.strip()]
        search_url = discover_clerk_search_url(session)
        asyncio.run(scrape_clerk(search_url, start, end, [], discover_terms=terms))
        return 0

    log.info("Collin County lead run | %s -> %s | %d doc type(s)%s",
             start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), len(codes),
             "" if args.no_foreclosures else " + foreclosure notices")

    # 1. Clerk portal ------------------------------------------------------
    raw_records: List[Dict[str, Any]] = []
    if args.no_clerk:
        existing = DATA_DIR / "records.json"
        if existing.exists():
            try:
                raw_records = json.loads(existing.read_text(encoding="utf-8")).get("records", [])
                log.info("Loaded %d existing record(s) from %s", len(raw_records), existing)
            except Exception as exc:  # noqa: BLE001
                log.error("Could not read %s: %s", existing, exc)
    else:
        search_url = discover_clerk_search_url(session)
        try:
            raw_records = asyncio.run(scrape_clerk(
                search_url, start, end, codes, with_details=not args.no_details)) or []
        except Exception as exc:  # noqa: BLE001 - a portal outage must not fail the job
            log.error("Clerk scrape failed: %s", exc)
            raw_records = []

    # 1b. County foreclosure notices ---------------------------------------
    if not args.no_foreclosures and not args.no_clerk:
        try:
            notices = asyncio.run(scrape_foreclosure_notices(start, end)) or []
            raw_records.extend(notices)
        except Exception as exc:  # noqa: BLE001 - optional source, never fatal
            log.error("Foreclosure notices failed: %s", exc)

    raw_records = dedupe(raw_records)
    log.info("%d unique record(s) after dedupe", len(raw_records))

    # 2. Property appraiser ------------------------------------------------
    parcel_index: Optional[ParcelIndex] = None
    if not args.no_parcels:
        try:
            parcel_index = build_parcel_index(session)
        except Exception as exc:  # noqa: BLE001
            log.error("Parcel index build failed: %s", exc)
            parcel_index = None

    # 3. Enrich, flag, score ----------------------------------------------
    records = enrich_and_score(raw_records, parcel_index)
    payload = build_payload(records, start, end)

    # 4. Outputs -----------------------------------------------------------
    write_outputs(payload)
    export_ghl_csv(payload, DATA_DIR / "ghl_export.csv")
    export_ghl_csv(payload, DASHBOARD_DIR / "ghl_export.csv")
    if args.export_csv:
        export_ghl_csv(payload, Path(args.export_csv))

    hot = sum(1 for r in payload["records"] if (r.get("score") or 0) >= 70)
    log.info("Done. total=%d with_address=%d hot(>=70)=%d",
             payload["total"], payload["with_address"], hot)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 - always exit clean for CI
        logging.getLogger("collin").exception("Unhandled error: %s", exc)
        sys.exit(0)
