#!/usr/bin/env python3
"""
Collect jobs from tyomarkkinatori.fi into Excel: JSON API listing, sync, missing Yritys.
/ Työmarkkinatori.fi → Excel: JSON-API-listaus, synkronointi, puuttuvat Yritys-kentät.

Flow: (1) POST /api/jobpostingfulltext/search/v2/search (same filters as listing URL)
      (2) Excel sync + save
      (3) Playwright only if Yritys still empty (JSON-LD / label on detail page)
/ Rakenne: (1) virallinen hakurajapinta
           (2) Excel-synk + tallennus
           (3) Playwright vain puuttuvaa Yritys varten
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from playwright.sync_api import Page, sync_playwright

# Paths and HTTP / Polku ja verkko
EXCEL_PATH = Path(__file__).resolve().parent / "tyomarkkinatori_jobs.xlsx"
BASE_DOMAIN = "https://tyomarkkinatori.fi"
# Listing template (used only to derive API filters). / Listaus-URL → API-suodattimet
LISTING_URL = (
    "https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat"
    "?in=25&or=CLOSING&p={p}&ps=30"
)
API_SEARCH_URL = f"{BASE_DOMAIN}/api/jobpostingfulltext/search/v2/search"
JOB_PATH_PREFIX = "/henkiloasiakkaat/avoimet-tyopaikat"

# Timeouts / Aikarajat
REQUEST_TIMEOUT_S = 60
PAGE_GOTO_TIMEOUT_MS = 45_000
CARD_PAGE_WAIT_MS = 1_500

# Save Excel after each successful Yritys update (crash safety).
SAVE_AFTER_EVERY_DETAIL_WRITE = True

# Max listing pages (safety). / Listasivujen yläraja
MAX_LISTING_PAGES = 600

# DataFrame columns / DataFrame-sarakkeet
DATA_COLUMNS = ["Linkki", "Tehtävänimike", "Yritys", "Työsuhde", "Työaika"]

# Company title suffix fallback / Otsikon Oy-tms.-fallback
COMPANY_SUFFIXES = (" oy", " oyj", " ab", " ltd", " ky", " tmi", " ry", " inc")
DEFAULT_CONTINUITY_LABELS = {
    "01": "toistaiseksi voimassa oleva",
    "02": "määräaikainen",
}
DEFAULT_WORKTIME_LABELS = {
    "01": "kokoaikainen",
    "02": "osa-aikainen",
}


def _ascii_fold(s: str) -> str:
    """Lowercase ASCII for Finnish city matching. / ASCII-pienet kirjaimet ää→a."""
    return (
        unicodedata.normalize("NFKD", (s or "").strip())
        .encode("ascii", "ignore")
        .decode()
        .lower()
        .strip()
    )


# Major Finnish cities (exact match only, ASCII-folded). No "short word" heuristic.
# / Suuret kaupungit: vain täsmäosuma; ei "lyhyt sana = kaupunki" -heuristiikkaa.
_FI_CITY_NAMES = (
    "Helsinki",
    "Espoo",
    "Tampere",
    "Vantaa",
    "Turku",
    "Oulu",
    "Jyväskylä",
    "Lahti",
    "Kuopio",
    "Pori",
    "Kouvola",
    "Joensuu",
    "Lappeenranta",
    "Hämeenlinna",
    "Vaasa",
    "Seinäjoki",
    "Rovaniemi",
    "Mikkeli",
    "Kotka",
    "Salo",
    "Hyvinkää",
    "Porvoo",
    "Kajaani",
    "Rauma",
    "Lohja",
    "Järvenpää",
    "Kirkkonummi",
    "Kerava",
    "Tuusula",
    "Savonlinna",
    "Riihimäki",
    "Valkeakoski",
)

FI_MAJOR_CITIES_ASCII = frozenset(
    _ascii_fold(c) for c in _FI_CITY_NAMES if _ascii_fold(c)
)


# ---------------------------------------------------------------------------
# URLs / URLit
# ---------------------------------------------------------------------------


def canonical_job_url(url: str) -> str:
    """Stable key: no query string, domain always present. / Yhtenäinen avain."""
    s = str(url).strip()
    if not s or s.lower() == "nan":
        return ""
    s = s.split("?", 1)[0].rstrip("/")
    if s and not s.startswith("http"):
        s = f"{BASE_DOMAIN.rstrip('/')}/{s.lstrip('/')}"
    return s


def job_url_from_api_id(job_id: str) -> str:
    """Public job page URL from API id. / Ilmoituksen sivun URL."""
    jid = (job_id or "").strip()
    if not jid:
        return ""
    return canonical_job_url(f"{BASE_DOMAIN}{JOB_PATH_PREFIX}/{jid}")


# ---------------------------------------------------------------------------
# API: search body from LISTING_URL / Hakukysely listaus-URL:sta
# ---------------------------------------------------------------------------


def _search_request_body(page_number: int) -> tuple[dict[str, Any], int]:
    """
    Build JSON body like the site widget (short query params → filters).
    Supported: q, in (iscoNotations), or (sorting), p, ps.
    / Rakenna sama pyyntö kuin sivusto: q, in, or, p, ps.
    """
    filled = LISTING_URL.format(p=page_number)
    parsed = urlparse(filled)
    q = parse_qs(parsed.query, keep_blank_values=True)
    page_size = int((q.get("ps") or ["30"])[0])
    sorting = (q.get("or") or ["CLOSING"])[0]
    query_text = (q.get("q") or [""])[0]
    filters: dict[str, Any] = {}
    if q.get("in"):
        filters["iscoNotations"] = q["in"]
    body: dict[str, Any] = {
        "query": query_text,
        "filters": filters,
        "paging": {"pageNumber": page_number, "pageSize": page_size},
        "sorting": sorting,
    }
    return body, page_size


def _employer_name_from_api_item(item: dict[str, Any]) -> str:
    """employer.ownerName.{fi,sv,en} or employer.name. / Työnantajan nimi API-kentistä."""
    emp = item.get("employer") or {}
    on = emp.get("ownerName")
    if isinstance(on, dict):
        for lang in ("fi", "sv", "en"):
            v = (on.get(lang) or "").strip()
            if v:
                return v[:500]
        for v in on.values():
            v = (str(v) or "").strip()
            if v:
                return v[:500]
    n = (emp.get("name") or "").strip()
    if n:
        return n[:500]
    return ""


def _title_from_api_item(item: dict[str, Any]) -> str:
    t = item.get("title")
    if isinstance(t, dict):
        for lang in ("fi", "sv", "en"):
            v = (t.get(lang) or "").strip()
            if v:
                return v[:500]
        for v in t.values():
            v = (str(v) or "").strip()
            if v:
                return v[:500]
    return (str(t) if t else "").strip()[:500]


def _continuity_code_to_tyosuhde(code: str) -> str:
    """Map continuity code to contract type label. / Muunna jatkuvuuskoodi työsuhde-tekstiksi."""
    c = (code or "").strip()
    if not c:
        return ""
    if c.startswith("01"):
        return DEFAULT_CONTINUITY_LABELS["01"]
    if c.startswith("02"):
        return DEFAULT_CONTINUITY_LABELS["02"]
    return ""


def _worktime_code_to_tyoaika(code: str) -> str:
    """Map worktime code to worktime label. / Muunna työaikakoodi työaika-tekstiksi."""
    c = (code or "").strip()
    if not c:
        return ""
    if c.startswith("01"):
        return DEFAULT_WORKTIME_LABELS["01"]
    if c.startswith("02"):
        return DEFAULT_WORKTIME_LABELS["02"]
    return ""


def fetch_continuity_labels(
    session: requests.Session,
) -> dict[str, str]:
    """
    Load TYÖN_JATKUVUUS code labels from API.
    / Hae TYÖN_JATKUVUUS-koodien selitteet API:sta.
    """
    url = (
        f"{BASE_DOMAIN}/api/codes/v1/kopa/TY%C3%96N_JATKUVUUS/koodit"
        f"?voimassa={date.today().isoformat()}"
    )
    out: dict[str, str] = {}
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT_S)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return out

    if not isinstance(payload, list):
        return out
    for row in payload:
        if not isinstance(row, dict):
            continue
        code = str(row.get("tunnus") or "").strip()
        labels = row.get("selite") or []
        if not code or not isinstance(labels, list):
            continue
        fi = ""
        for item in labels:
            if not isinstance(item, dict):
                continue
            if (item.get("kielikoodi") or "").strip().lower() == "fi":
                fi = str(item.get("teksti") or "").strip()
                break
        if fi:
            # Normalize output to the two required classes.
            out[code] = _continuity_code_to_tyosuhde(code) or fi.lower()
    return out


def fetch_worktime_labels(
    session: requests.Session,
) -> dict[str, str]:
    """
    Load TYÖAIKA code labels from API.
    / Hae TYÖAIKA-koodien selitteet API:sta.
    """
    url = (
        f"{BASE_DOMAIN}/api/codes/v1/kopa/TY%C3%96AIKA/koodit"
        f"?voimassa={date.today().isoformat()}"
    )
    out: dict[str, str] = {}
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT_S)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return out

    if not isinstance(payload, list):
        return out
    for row in payload:
        if not isinstance(row, dict):
            continue
        code = str(row.get("tunnus") or "").strip()
        labels = row.get("selite") or []
        if not code or not isinstance(labels, list):
            continue
        fi = ""
        for item in labels:
            if not isinstance(item, dict):
                continue
            if (item.get("kielikoodi") or "").strip().lower() == "fi":
                fi = str(item.get("teksti") or "").strip()
                break
        if fi:
            out[code] = _worktime_code_to_tyoaika(code) or fi.lower()
    return out


def _tyosuhde_from_api_item(
    item: dict[str, Any],
    continuity_labels: dict[str, str],
) -> str:
    """Työsuhde from continuityOfWork code list. / Työsuhde continuityOfWork-koodista."""
    raw = item.get("continuityOfWork") or []
    if not isinstance(raw, list):
        raw = [raw]
    for code in raw:
        c = str(code or "").strip()
        if not c:
            continue
        mapped = continuity_labels.get(c) or _continuity_code_to_tyosuhde(c)
        if mapped:
            return mapped[:120]
    return ""


def _tyoaika_from_api_item(
    item: dict[str, Any],
    worktime_labels: dict[str, str],
) -> str:
    """Työaika from workTime code. / Työaika workTime-koodista."""
    code = str(item.get("workTime") or "").strip()
    if not code:
        return ""
    mapped = worktime_labels.get(code) or _worktime_code_to_tyoaika(code)
    return mapped[:120] if mapped else ""


def job_row_from_api_item(
    item: dict[str, Any],
    continuity_labels: dict[str, str],
    worktime_labels: dict[str, str],
) -> dict[str, str]:
    """One row: Linkki, Tehtävänimike, Yritys from API hit. / Yksi rivi API:sta."""
    jid = (item.get("id") or "").strip()
    link = job_url_from_api_id(jid)
    title = _title_from_api_item(item) or "-"
    yritys = clean_company_name(_employer_name_from_api_item(item))
    tyosuhde = _tyosuhde_from_api_item(item, continuity_labels)
    tyoaika = _tyoaika_from_api_item(item, worktime_labels)
    if looks_like_location(yritys):
        yritys = ""
    return {
        "Linkki": link,
        "Tehtävänimike": title,
        "Yritys": yritys,
        "Työsuhde": tyosuhde,
        "Työaika": tyoaika,
    }


def fetch_all_listings_api(session: Optional[requests.Session] = None) -> list[dict]:
    """
    All jobs via search API (fast, structured employer names).
    / Kaikki ilmoitukset hakurajapinnasta.
    """
    sess = session or requests.Session()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Referer": f"{BASE_DOMAIN}/",
    }
    all_jobs: list[dict] = []
    page_num = 0
    continuity_labels = fetch_continuity_labels(sess)
    worktime_labels = fetch_worktime_labels(sess)

    while page_num < MAX_LISTING_PAGES:
        body, _ = _search_request_body(page_num)
        print(f"Haetaan API-sivu {page_num + 1}...", flush=True)
        try:
            r = sess.post(
                API_SEARCH_URL,
                json=body,
                headers=headers,
                timeout=REQUEST_TIMEOUT_S,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"API-virhe (sivu {page_num + 1}): {e}", flush=True)
            break

        content = data.get("content") or []
        if not content:
            break

        for item in content:
            if isinstance(item, dict):
                all_jobs.append(
                    job_row_from_api_item(item, continuity_labels, worktime_labels)
                )

        print(
            f"Löytyi {len(content)} ilmoitusta (yhteensä {len(all_jobs)}).",
            flush=True,
        )
        if len(content) < body["paging"]["pageSize"]:
            break
        page_num += 1

    return all_jobs


# ---------------------------------------------------------------------------
# Job page: Yritys fallback (Playwright) / Ilmoitussivu: varayritys
# ---------------------------------------------------------------------------


def clean_company_name(value: str) -> str:
    """Return plausible company string or empty. / Kelvollinen yritysnimi tai tyhjä."""
    s = (value or "").strip()
    if not s or len(s) > 80:
        return ""
    low = s.lower()
    if any(
        t in low
        for t in (
            " toimii ",
            " yrityksessä",
            " tehtävässä",
            " rooli ",
            " tiimi ",
        )
    ):
        return ""
    return s


def looks_like_location(value: str) -> bool:
    """
    True only for exact major-city match or obvious multi-city location strings.
    No generic "short single word" rule.
    / Vain täsmäkaupunki tai selvä monen kaupungin -merkkijono.
    """
    s = (value or "").strip()
    if not s:
        return False
    key = _ascii_fold(s)
    if key in FI_MAJOR_CITIES_ASCII:
        return True
    low = s.lower()
    remote_exact = (
        "koko suomi",
        "koko suomessa",
        "100% etätyö",
        "100% etatyö",
        "etätyö",
        "etatyö",
    )
    if low in remote_exact:
        return True
    if re.search(r"\s+or\s+|\s+tai\s+", low):
        parts = [p.strip() for p in re.split(r"\s+or\s+|\s+tai\s+", low) if p.strip()]
        if len(parts) >= 2 and all(
            _ascii_fold(p) in FI_MAJOR_CITIES_ASCII for p in parts
        ):
            return True
    return False


def company_from_title_fallback(title: str) -> str:
    """Last comma segment if legal suffix (Oy, …). / Viimeinen pilkkuerotettu vain jos Oy-tms."""
    t = (title or "").strip()
    if not t or "," not in t:
        return ""
    cand = t.split(",")[-1].strip()
    cand = clean_company_name(cand)
    if not cand or looks_like_location(cand):
        return ""
    low = f" {cand.lower()}"
    if any(tok in low for tok in COMPANY_SUFFIXES):
        return cand
    return ""


def extract_company_json_ld_and_label(page: Page) -> str:
    """JSON-LD hiringOrganization, then Yritys label before location. / JSON-LD + Yritys-teksti."""
    try:
        return (
            page.evaluate(
                """
            () => {
                const clean = (v) => (v || '').toString().trim();
                const ldScripts = Array.from(
                    document.querySelectorAll('script[type="application/ld+json"]')
                );
                for (const s of ldScripts) {
                    try {
                        const raw = clean(s.textContent);
                        if (!raw) continue;
                        const data = JSON.parse(raw);
                        const items = Array.isArray(data) ? data : [data];
                        for (const item of items) {
                            const org = item && item.hiringOrganization;
                            const name = clean(org && org.name);
                            if (name) return name;
                        }
                    } catch (_) {}
                }
                const bodyText = clean(document.body && document.body.innerText);
                if (bodyText) {
                    const headEnd = bodyText.toLowerCase()
                        .indexOf('työpaikan sijainti');
                    const block = headEnd >= 0
                        ? bodyText.slice(0, headEnd)
                        : bodyText;
                    const m = block.match(/Yritys\\s*[:\\s\\u00a0]+([^\\n]+)/i);
                    if (m && m[1]) return clean(m[1]);
                }
                return '';
            }
            """
            )
            or ""
        ).strip()
    except Exception:
        return ""


def fetch_yritys_from_job_page(page: Page, url: str, title_hint: str = "") -> str:
    """Open job page; Yritys from JSON-LD / label, else title suffix. / Avaa sivu, poimi Yritys."""
    s = (url or "").strip()
    full = s if s.startswith("http") else f"{BASE_DOMAIN.rstrip('/')}/{s.lstrip('/')}"
    try:
        page.goto(full, wait_until="load", timeout=PAGE_GOTO_TIMEOUT_MS)
        page.wait_for_timeout(CARD_PAGE_WAIT_MS)
    except Exception:
        return ""

    y = clean_company_name(extract_company_json_ld_and_label(page))
    if looks_like_location(y):
        y = ""
    if not y:
        y = company_from_title_fallback(title_hint)
    return y


# ---------------------------------------------------------------------------
# Excel / Excel
# ---------------------------------------------------------------------------


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure Linkki, Tehtävänimike, Yritys. / Varmista sarakkeet."""
    for col in DATA_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def extract_urls_and_titles_from_excel(path: Path, n_rows: int) -> tuple[list[str], list[str]]:
    """URLs and titles from Tehtävänimike hyperlinks. / URL ja otsikko linkeistä."""
    urls: list[str] = []
    titles: list[str] = []
    try:
        wb = load_workbook(path, data_only=False)
        ws = wb.active
        col_idx = 1
        for c in range(1, ws.max_column + 1):
            if ws.cell(row=1, column=c).value == "Tehtävänimike":
                col_idx = c
                break
        for r in range(2, ws.max_row + 1):
            if len(urls) >= n_rows:
                break
            cell = ws.cell(row=r, column=col_idx)
            href = None
            title = ""
            val = cell.value
            h = getattr(cell, "hyperlink", None)
            if h:
                href = getattr(h, "target", None) or getattr(h, "location", None)
            if isinstance(val, str) and "HYPERLINK" in val:
                m_url = re.search(r'HYPERLINK\s*\(\s*"((?:[^"]|"")+)"', val, re.I)
                if m_url:
                    href = href or m_url.group(1).replace('""', '"')
                m_title = re.search(
                    r'HYPERLINK\s*\(\s*"(?:[^"]|"")*"\s*,\s*"((?:[^"]|"")*)"\s*\)',
                    val,
                    re.I,
                )
                if m_title:
                    title = m_title.group(1).replace('""', '"').strip()
            elif isinstance(val, str) and val.strip() and not val.strip().startswith("="):
                title = val.strip()[:500]
            urls.append(str(href or "").strip())
            titles.append(title)
        wb.close()
    except Exception:
        pass
    return urls, titles


def sync_dataframe(
    jobs_from_web: list[dict],
) -> tuple[pd.DataFrame, int, int, set[str]]:
    """DataFrame = only jobs on site. / Taulukko = vain sivulla olevat."""
    web_links = {canonical_job_url(j["Linkki"]) for j in jobs_from_web}
    web_links.discard("")

    if not EXCEL_PATH.exists():
        df = ensure_columns(pd.DataFrame(jobs_from_web))
        return df, len(jobs_from_web), 0, set()

    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        print(f"Excelin lukuvirhe: {e}", flush=True)
        df = ensure_columns(pd.DataFrame(jobs_from_web))
        return df, len(jobs_from_web), 0, set()

    df = ensure_columns(df)
    if "Tehtävänimike" in df.columns and df["Tehtävänimike"].dtype != object:
        df["Tehtävänimike"] = df["Tehtävänimike"].astype(object)
    for col in ("Yritys", "Työsuhde", "Työaika"):
        if col in df.columns and df[col].dtype != object:
            df[col] = df[col].astype(object)

    urls, titles = extract_urls_and_titles_from_excel(EXCEL_PATH, len(df))
    if len(urls) < len(df):
        urls.extend([""] * (len(df) - len(urls)))
        titles.extend([""] * (len(df) - len(titles)))
    df["Linkki"] = urls[: len(df)]

    def is_empty_title(x: object) -> bool:
        if pd.isna(x):
            return True
        s = str(x).strip().lower()
        return not s or s == "nan"

    for i, tit in enumerate(titles[: len(df)]):
        if tit and tit.lower() != "nan" and is_empty_title(df.at[i, "Tehtävänimike"]):
            df.at[i, "Tehtävänimike"] = tit

    excel_links = {
        canonical_job_url(x) for x in df["Linkki"].astype(str).unique() if canonical_job_url(x)
    }
    new_links = web_links - excel_links
    removed_links = excel_links - web_links

    df = df[~df["Linkki"].astype(str).apply(canonical_job_url).isin(removed_links)].copy()
    new_rows = [j for j in jobs_from_web if canonical_job_url(j["Linkki"]) in new_links]
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        for c in df.columns:
            if c not in new_df.columns:
                new_df[c] = ""
        new_df = new_df.reindex(columns=df.columns, fill_value="")
        df = pd.concat([df, new_df], ignore_index=True)

    df = df[df["Linkki"].astype(str).apply(canonical_job_url).isin(web_links)].copy()
    df = df.drop_duplicates(subset=["Linkki"], keep="first")

    link_to_title = {
        canonical_job_url(j["Linkki"]): j.get("Tehtävänimike", "") for j in jobs_from_web
    }
    for i in df.index:
        if is_empty_title(df.at[i, "Tehtävänimike"]):
            t = link_to_title.get(canonical_job_url(str(df.at[i, "Linkki"])), "-")
            if t and str(t).strip().lower() != "nan":
                df.at[i, "Tehtävänimike"] = str(t)

    link_to_yritys: dict[str, str] = {}
    link_to_tyosuhde: dict[str, str] = {}
    link_to_tyoaika: dict[str, str] = {}
    for j in jobs_from_web:
        lk = canonical_job_url(j["Linkki"])
        if not lk:
            continue
        raw_y = str(j.get("Yritys", "") or "").strip()
        if not raw_y:
            continue
        y = clean_company_name(raw_y)
        if y and not looks_like_location(y):
            link_to_yritys[lk] = y
        ts = str(j.get("Työsuhde", "") or "").strip()
        if ts:
            link_to_tyosuhde[lk] = ts
        ta = str(j.get("Työaika", "") or "").strip()
        if ta:
            link_to_tyoaika[lk] = ta
    for i in df.index:
        y = link_to_yritys.get(canonical_job_url(str(df.at[i, "Linkki"])), "")
        if y:
            df.at[i, "Yritys"] = y
        ts = link_to_tyosuhde.get(canonical_job_url(str(df.at[i, "Linkki"])), "")
        if ts:
            df.at[i, "Työsuhde"] = ts
        ta = link_to_tyoaika.get(canonical_job_url(str(df.at[i, "Linkki"])), "")
        if ta:
            df.at[i, "Työaika"] = ta

    order = {canonical_job_url(j["Linkki"]): i for i, j in enumerate(jobs_from_web)}
    df["_ord"] = df["Linkki"].astype(str).apply(
        lambda x: order.get(canonical_job_url(x), 999)
    )
    df = df.sort_values("_ord").drop(columns=["_ord"])
    df.reset_index(drop=True, inplace=True)
    return df, len(new_rows), len(removed_links), removed_links


def row_has_valid_yritys(df: pd.DataFrame, i: int) -> bool:
    """Non-empty Yritys and not classified as location. / Yritys ok."""
    if "Yritys" not in df.columns:
        return False
    v = df.at[i, "Yritys"]
    if v is None or not str(v).strip():
        return False
    return not looks_like_location(str(v))


def get_url_from_hyperlink_cell(cell) -> Optional[str]:
    h = getattr(cell, "hyperlink", None)
    if h:
        return getattr(h, "target", None) or getattr(h, "location", None)
    if isinstance(cell.value, str) and cell.value.startswith("=HYPERLINK("):
        m = re.search(r'=HYPERLINK\s*\(\s*"((?:[^"]|"")+)"', cell.value)
        if m:
            return m.group(1).replace('""', '"')
    return None


def find_title_column_index(ws) -> int:
    for c in range(1, ws.max_column + 1):
        if ws.cell(row=1, column=c).value == "Tehtävänimike":
            return c
    return 1


def save_workbook_atomic(wb, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".xlsx", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        wb.save(tmp_path)
        os.replace(tmp_path, path)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise
    finally:
        wb.close()


def save_excel(df: pd.DataFrame) -> None:
    df = ensure_columns(df)
    display_cols = ["Tehtävänimike", "Yritys", "Työsuhde", "Työaika"]

    if not EXCEL_PATH.exists():
        df_out = df.reindex(columns=display_cols, fill_value="")
        df_out = df_out[display_cols]
        df_out.to_excel(EXCEL_PATH, index=False, engine="openpyxl")
        apply_hyperlinks_new_file(EXCEL_PATH, df["Linkki"].astype(str).tolist())
        return

    wb = load_workbook(EXCEL_PATH)
    ws = wb.active
    title_col = find_title_column_index(ws)

    link_to_row: dict[str, int] = {}
    for r in range(2, ws.max_row + 1):
        cell = ws.cell(row=r, column=title_col)
        url = get_url_from_hyperlink_cell(cell)
        if url:
            nu = canonical_job_url(url)
            if nu:
                link_to_row[nu] = r

    wanted = {canonical_job_url(str(row.get("Linkki", ""))) for _, row in df.iterrows()}
    wanted.discard("")
    keep_rows = {r for url, r in link_to_row.items() if url in wanted}
    to_delete = sorted(
        (r for r in range(2, ws.max_row + 1) if r not in keep_rows),
        reverse=True,
    )
    if to_delete:
        print(f"Poistetaan {len(to_delete)} riviä (vanhentuneet + tyhjät).", flush=True)
    for row_idx in to_delete:
        ws.delete_rows(row_idx, 1)
        for k in list(link_to_row):
            if link_to_row[k] > row_idx:
                link_to_row[k] -= 1
            elif link_to_row[k] == row_idx:
                del link_to_row[k]

    col_indices: dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        h = ws.cell(row=1, column=c).value
        if h in display_cols:
            col_indices[str(h)] = c
    for col in display_cols:
        if col not in col_indices:
            new_c = ws.max_column + 1
            ws.cell(row=1, column=new_c, value=col)
            col_indices[col] = new_c

    for _, row in df.iterrows():
        link_raw = str(row.get("Linkki", "")).strip()
        if not link_raw or link_raw == "nan":
            continue
        link = canonical_job_url(link_raw)
        if not link:
            continue

        if link in link_to_row:
            excel_row = link_to_row[link]
            for col in display_cols:
                if col == "Tehtävänimike":
                    continue
                if col in col_indices and col in row:
                    val = row[col]
                    if pd.notna(val) and str(val).strip():
                        ws.cell(
                            row=excel_row, column=col_indices[col]
                        ).value = str(val)[:500]
        else:
            new_row = ws.max_row + 1
            for col in display_cols:
                if col in col_indices and col in row:
                    val = row[col] if col != "Tehtävänimike" else row.get(
                        "Tehtävänimike", "-"
                    )
                    if pd.notna(val) and str(val).strip():
                        ws.cell(row=new_row, column=col_indices[col]).value = str(
                            val
                        )[:500]
            set_hyperlink_cell(
                ws,
                new_row,
                title_col,
                link,
                display=str(row.get("Tehtävänimike", "-")),
            )
            link_to_row[link] = new_row

    apply_hyperlinks_to_worksheet(ws, df, title_col)

    last_data_row = 1
    for r in range(2, ws.max_row + 1):
        if ws.cell(row=r, column=title_col).value:
            last_data_row = r
    while ws.max_row > last_data_row:
        ws.delete_rows(ws.max_row, 1)

    if ws.max_column >= 1:
        ws.column_dimensions["A"].width = 60
    if ws.max_column >= 2:
        ws.column_dimensions["B"].width = 80
    if ws.max_column >= 3:
        ws.column_dimensions["C"].width = 36
    if ws.max_column >= 4:
        ws.column_dimensions["D"].width = 24
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"

    save_workbook_atomic(wb, EXCEL_PATH)
    print(f"Tallennettu: {last_data_row - 1} riviä.", flush=True)


def apply_hyperlinks_new_file(path: Path, urls: list[str]) -> None:
    try:
        wb = load_workbook(path)
        ws = wb.active
        title_col = find_title_column_index(ws)
        for r in range(2, min(ws.max_row + 1, len(urls) + 2)):
            idx = r - 2
            if idx < len(urls) and urls[idx]:
                set_hyperlink_cell(ws, r, title_col, urls[idx])
        save_workbook_atomic(wb, path)
    except Exception:
        pass


def set_hyperlink_cell(
    ws,
    row: int,
    col: int,
    url: str,
    display: Optional[str] = None,
) -> None:
    s = str(url).strip()
    if ".fihenkilo" in s:
        s = s.replace(".fihenkilo", ".fi/henkilo", 1)
    elif not s.startswith("http"):
        s = f"{BASE_DOMAIN.rstrip('/')}/{s.lstrip('/')}"
    cell = ws.cell(row=row, column=col)
    raw = display if display is not None else cell.value
    label = (
        "-"
        if (raw is None or str(raw).strip().lower() in ("", "nan"))
        else str(raw)
    ).replace('"', '""')
    url_esc = s.replace('"', '""')
    cell.value = f'=HYPERLINK("{url_esc}", "{label}")'
    cell.font = Font(color="0563C1", underline="single")


def apply_hyperlinks_to_worksheet(ws, df: pd.DataFrame, title_col: int) -> None:
    linkki_to_row_data: dict[str, Any] = {}
    for _, r in df.iterrows():
        lk = canonical_job_url(str(r.get("Linkki", "")))
        if lk:
            linkki_to_row_data[lk] = r
    for r in range(2, ws.max_row + 1):
        cell = ws.cell(row=r, column=title_col)
        url = get_url_from_hyperlink_cell(cell)
        if url:
            nurl = canonical_job_url(url)
            if nurl in linkki_to_row_data:
                row_data = linkki_to_row_data[nurl]
                disp = row_data.get("Tehtävänimike")
                disp = "-" if (pd.isna(disp) or not str(disp).strip()) else str(disp)
                set_hyperlink_cell(ws, r, title_col, url, display=disp)


def fill_missing_yritys_with_browser(page: Page, df: pd.DataFrame) -> None:
    """Playwright only for rows still missing Yritys. / Selain vain puuttuville."""
    if "Yritys" in df.columns:
        for i in range(len(df)):
            if looks_like_location(str(df.at[i, "Yritys"])):
                df.at[i, "Yritys"] = ""

    n = len(df)
    skipped = 0
    for i in range(n):
        link = df.at[i, "Linkki"]
        if pd.isna(link) or not str(link).strip():
            continue
        if row_has_valid_yritys(df, i):
            skipped += 1
            continue

        print(f"Haetaan Yritys sivulta {i + 1}/{n}...", flush=True)
        try:
            title_hint = (
                str(df.at[i, "Tehtävänimike"])
                if "Tehtävänimike" in df.columns
                else ""
            )
            val = fetch_yritys_from_job_page(page, str(link).strip(), title_hint=title_hint)
            if val:
                df.at[i, "Yritys"] = val
                if SAVE_AFTER_EVERY_DETAIL_WRITE:
                    save_excel(df)
        except Exception as e:
            print(f"Rivi {i + 1}/{n} ohitettu: {e}", flush=True)

    if skipped:
        print(f"Ohitettiin {skipped} riviä (Yritys jo API:sta / kelvollinen).", flush=True)


def needs_browser_for_yritys(df: pd.DataFrame) -> bool:
    for i in range(len(df)):
        if not row_has_valid_yritys(df, i):
            lk = df.at[i, "Linkki"]
            if pd.notna(lk) and str(lk).strip():
                return True
    return False


# ---------------------------------------------------------------------------
# Entry point / Käynnistys
# ---------------------------------------------------------------------------


def main() -> None:
    print("Työmarkkinatori -synkronointi alkaa.", flush=True)
    df: Optional[pd.DataFrame] = None

    try:
        with requests.Session() as http:
            jobs = fetch_all_listings_api(http)
            if not jobs:
                print("API:sta ei löytynyt ilmoituksia.", flush=True)
                return

            print(f"API:sta yhteensä {len(jobs)} ilmoitusta.", flush=True)
            df, added, removed_n, _ = sync_dataframe(jobs)
            print(
                f"Synkronointi: +{added} uutta, -{removed_n} poistunutta.",
                flush=True,
            )
            save_excel(df)

            if needs_browser_for_yritys(df):
                print("Täydennetään puuttuvaa Yritys selaimella...", flush=True)
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    try:
                        context = browser.new_context(
                            user_agent=(
                                "Mozilla/5.0 (Windows NT 10.0; rv:109.0) "
                                "Gecko/20100101 Firefox/115.0"
                            ),
                        )
                        try:
                            page = context.new_page()
                            fill_missing_yritys_with_browser(page, df)
                        finally:
                            context.close()
                    finally:
                        browser.close()
                save_excel(df)

            print(
                "Synkronointi valmis. Tehtävänimike-sarake = hyperlink.",
                flush=True,
            )

    except KeyboardInterrupt:
        print("Keskeytetty.", flush=True)
        if df is not None:
            save_excel(df)
        sys.exit(1)
    except Exception as e:
        print(f"Virhe: {e}", flush=True)
        if df is not None:
            try:
                save_excel(df)
            except Exception:
                pass
        raise


if __name__ == "__main__":
    main()
