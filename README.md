# Collin County, TX — Motivated Seller Lead Scraper

Pulls distressed-property filings from the Collin County Clerk's Official Public
Records **and** the county's foreclosure-notice app, joins them to CollinCAD
parcel data for property + mailing addresses, scores each lead 0–100, and
publishes a dashboard plus a GoHighLevel-ready CSV.

A typical 7-day run: ~240 leads, ~170 with an address, ~60 of them active
notices of foreclosure sale.

```
scraper/fetch.py            the whole pipeline
scraper/requirements.txt
dashboard/index.html        filterable lead dashboard (GitHub Pages)
dashboard/records.json      latest results (also written to data/records.json)
dashboard/ghl_export.csv    GHL contact import (also data/ghl_export.csv)
.github/workflows/scrape.yml  daily cron at 07:00 UTC + manual dispatch
```

## Run it locally

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

Useful flags:

```bash
python scraper/fetch.py --lookback 14        # wider window
python scraper/fetch.py --types LP,ML,AJ     # only these document types
python scraper/fetch.py --no-parcels         # skip the 170 MB CAD download
python scraper/fetch.py --no-details         # skip per-document detail pages
python scraper/fetch.py --no-clerk           # re-score/re-join existing records
python scraper/fetch.py --no-foreclosures    # skip the foreclosure-notice app
python scraper/fetch.py --debug              # verbose + headful browser
```

## How it works

**1. Clerk portal (Playwright, async).** The clerk page links to the county's
records app at `https://collin.tx.publicsearch.us` (Kofile/GovOS PublicSearch).
`discover_clerk_search_url()` finds it from the landing page and falls back to
that URL. Its advanced search is URL-drivable:

```
/results?department=RP&docTypes=LP&recordedDateRange=20260827%2C20260903
        &searchType=advancedSearch&limit=50&offset=0
```

Results are paged 50 at a time (the app rejects larger limits). Each row carries
the portal's internal document id in its checkbox, which is also the permalink —
`/doc/330542281` — and that becomes `clerk_url`. Kept records then get their
detail page read for consideration amount and the full party list.

If the county ever changes vendors, `scrape_clerk()` falls back to a generic
form-filling path that handles typical ASP.NET record portals (disclaimer gates,
date fields, document-type selects, "Next" pagination).

**2. Document type codes.** The portal filters on its own internal codes.
Verified Collin codes are in `VERIFIED_CODES` — `LP` (lis pendens), `AJ`/`JD`/
`ABSJ` (judgments), `LIEN`/`ML`/`PN`/`CSL` (liens), `FTL`/`STL`/`PRSTLI` (tax
liens), `PROB`/`AH`/`DB` (probate, heirship, death certificate). The lead-type
codes from the original spec (`NOFC`, `LNMECH`, `PRO`, …) are kept as aliases in
`SPEC_ALIASES` so they still classify correctly, but Collin does not use them. To re-derive them after a vocabulary change:

```bash
python scraper/fetch.py --discover-types "LIEN,JUDGMENT,PENDENS,PROBATE"
```

It drives Advanced Search, selects each matching document type, and reads the
code back out of the results URL. Feed the output to `CLERK_DOC_TYPES`.

Anything the code filter returns is *also* re-classified locally from the
portal's own description text (`DESCRIPTION_PATTERNS`), so a filing lands in the
right lead category even when the code is unfamiliar.

**3. Foreclosure notices.** Collin's clerk index carries no trustee-sale
document type — a 30-day sweep of its generic `NOTICE` type returned 753 filings
and zero foreclosures. The county runs a separate Blazor app for them, linked
from the clerk page as "Foreclosures":

```
https://apps2.collincountytx.gov/ForeclosureNotices
```

`scrape_foreclosure_notices()` tries the app's "Filed Date Start/End" filters
(filling rather than clicking them, since MudBlazor's date picker opens an
overlay that swallows the next click), verifies whether the filter actually took
by checking the rows on screen, and otherwise pages the full list — 37 pages,
about a minute — and filters by filed date locally. Either way it reads address,
city, sale date, filed date and property type from every row. Those notices name no owner, which is what the
parcel *address* index is for — see below.

**4. CollinCAD parcel data.** `collincad.org` links its bulk files through
`link.collincad.org`; a plain GET returns the zip (a `HEAD` misleadingly returns
HTML). The scraper ranks the candidates, downloads the best one — currently
`parcels_with_appraisal_data_R5.zip`, ~170 MB, 440k parcels — and reads the DBF
with `dbfread`. ASP.NET `__doPostBack` download buttons are handled too
(`_download_via_postback`) for pages that hand files back only through a form POST.

`ParcelIndex` holds two views of the 440k parcels:

- **by owner** — every name ordering (`FIRST LAST`, `LAST FIRST`, `LAST, FIRST`,
  plus first+last-only forms), so a clerk grantor matches however it was typed;
- **by address** — normalized street address (and address+ZIP), which turns an
  ownerless foreclosure notice into a named lead with a mailing address.

Columns are matched against both the generic CAMA names (`OWNER`/`OWN1`,
`SITE_ADDR`, `ADDR_1`, …) and the names CollinCAD actually ships (`file_as_na`,
`situs_*`, `addr_line1..3`); the schema is resolved once per table rather than
per row, which is the difference between a 90-second and a 4-minute index build.

**5. Picking the lead.** Filings are indexed inconsistently — a creditor, HOA or
taxing authority files *against* the owner, so the owner is usually the grantee,
while probate filings list them first. `resolve_lead_party()` ranks every party
(individuals before companies, companies before banks/HOAs/agencies; grantee side
before grantor side) and prefers whichever candidate actually owns Collin County
property according to the parcel index. That keeps "FERGUSON ENTERPRISES LLC"
(the judgment creditor) out of the owner column and puts the debtor there.

**6. Scoring.** Base 30, +10 per flag, +20 for a lis-pendens + foreclosure combo
on the same owner, +15 if amount > $100k (else +10 if > $50k), +5 if filed in the
last 7 days, +5 if an address was found. Clamped to 0–100. Flags: Lis pendens,
Pre-foreclosure, Judgment lien, Tax lien, Mechanic lien, Probate / estate,
LLC / corp owner, New this week.

## Output

`dashboard/records.json` and `data/records.json`:

```json
{ "fetched_at": "...Z", "source": "...", "date_range": "2026-08-28 to 2026-09-04",
  "total": 4, "with_address": 3,
  "records": [{ "doc_num": "...", "doc_type": "...", "filed": "...", "cat": "...",
                "cat_label": "...", "owner": "...", "grantee": "...", "amount": null,
                "legal": "...", "prop_address": "...", "prop_city": "...",
                "prop_state": "...", "prop_zip": "...", "mail_address": "...",
                "mail_city": "...", "mail_state": "...", "mail_zip": "...",
                "clerk_url": "...", "flags": ["..."], "score": 75 }] }
```

`ghl_export.csv` columns: First Name, Last Name, Mailing Address, Mailing City,
Mailing State, Mailing Zip, Property Address, Property City, Property State,
Property Zip, Lead Type, Document Type, Date Filed, Document Number,
Amount/Debt Owed, Seller Score, Motivated Seller Flags, Source, Public Records URL.

## Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `LOOKBACK_DAYS` | `7` | Days of filings to pull |
| `CLERK_PORTAL_URL` | county clerk page | Where discovery starts |
| `CLERK_SEARCH_URL` | *(auto)* | Skip discovery, use this search app |
| `CLERK_DOC_TYPES` | *(all in `DOC_TYPES`)* | Comma-separated codes to query |
| `CLERK_DEPARTMENT` | `RP` | PublicSearch department (RP = real property) |
| `CLERK_PAGE_LIMIT` | `50` | Results per page (the app caps at 50) |
| `MAX_DETAIL_FETCH` | `200` | Per-document detail pages per run |
| `MAX_PAGES_PER_TYPE` | `25` | Pagination guard |
| `FORECLOSURE_URL` | county notices app | Foreclosure-notice source |
| `MAX_FORECLOSURE_PAGES` | `40` | Pagination guard for the notice app |
| `CAD_BASE_URL` | `https://collincad.org/` | Where parcel discovery starts |
| `CAD_DBF_URL` | *(auto)* | Direct .zip/.dbf URL, skips discovery |
| `CAD_LOCAL_FILE` | – | Use a local .zip/.dbf instead of downloading |
| `CAD_CACHE_HOURS` | `20` | Reuse the cached bulk file for this long |
| `RETRIES` | `3` | Attempts per network operation |
| `HEADLESS` | `1` | `0` to watch the browser |

## Automation

`.github/workflows/scrape.yml` runs daily at 07:00 UTC (and on demand), installs
Chromium, runs the scraper, writes a run summary, commits the JSON/CSV outputs,
and deploys `dashboard/` to GitHub Pages. Enable **Settings → Pages → Source:
GitHub Actions** once.

## Notes and limits

- Records with no CAD match keep every clerk field; only the address columns are
  blank. `with_address` in the JSON tells you the join rate for the run.
- Amounts come from the document's *Consideration* field, which many liens leave
  empty — a blank amount is normal, not a bug.
- Nothing here crashes the job: a portal outage, a bad row, or a failed CAD
  download all degrade to a smaller result set with a valid `records.json`.
- Clerk volume is genuinely low — a typical week has a handful of lis pendens
  filings. Foreclosure notices are where the bulk of the leads come from.
- A full 7-day run takes roughly 8–9 minutes: most of it is the per-document
  detail pages and the 37-page foreclosure list. `--no-details` cuts it in half
  at the cost of consideration amounts.
