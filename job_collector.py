#!/usr/bin/env python3
"""
Collect jobs from tyomarkkinatori.fi into Excel: listing, sync, missing Yritys.
/ Työmarkkinatori.fi → Excel: listaus, synkronointi, puuttuvat Yritys-kentät.

Flow: (1) listing pages → Linkki, Tehtävänimike, Yritys from card
      (2) Excel sync + save
      (3) open job pages only for rows with missing or bad Yritys (JSON-LD / label only)
/ Rakenne: (1) listasivut → Linkki, Tehtävänimike, Yritys kortilta
           (2) Excel-synk + tallennus
           (3) avaa ilmoitus vain puuttuvaa Yritys varten (ei täyttä korttiparsintaa)
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from playwright.sync_api import Page, sync_playwright

# Paths and HTTP / Polku ja verkko
EXCEL_PATH = Path(__file__).resolve().parent / "tyomarkkinatori_jobs.xlsx"
BASE_DOMAIN = "https://tyomarkkinatori.fi"
LISTING_URL = (
    "https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat"
    "?in=25&or=CLOSING&p={p}&ps=30"
)

# Timeouts (ms): avoid hangs; not too long / Aikarajat (ms): ei jäätymistä, ei turhia odotuksia
PAGE_GOTO_TIMEOUT_MS = 45_000
LISTING_SELECTOR_TIMEOUT_MS = 20_000
CARD_PAGE_WAIT_MS = 1_500

# Save Excel after each successful Yritys update (crash safety).
# / Tallenna Excel jokaisen onnistuneen Yritys-päivityksen jälkeen (turva sammumista vastaan).
SAVE_AFTER_EVERY_DETAIL_WRITE = True

PAGE_SIZE = 30

# DataFrame + Excel columns we use (no full job-card field dump).
# / Käytetyt sarakkeet (ei koko ilmoituksen kenttälistaa).
DATA_COLUMNS = ["Linkki", "Tehtävänimike", "Yritys"]

# Listing page: distinguish job links from list URL / Listasivu: työlinkki vs. lista-URL
RE_LISTING_PAGE = re.compile(
    r"^https?://[^/]+/henkiloasiakkaat/avoimet-tyopaikat/\?", re.I
)
RE_JOB_PATH = re.compile(r"/avoimet-tyopaikat/[^/?]+", re.I)

# Company vs location heuristics / Yritys vs. sijainti -heuristiikat
COMPANY_SUFFIXES = (" oy", " oyj", " ab", " ltd", " ky", " tmi", " ry", " inc")
LOCATION_SUBSTRINGS = (
    "helsinki",
    "espoo",
    "vantaa",
    "tampere",
    "turku",
    "hämeenlinna",
    "koko suomi",
    "suomi",
    "tai",
    "etätyö",
    "etatyö",
)


# ---------------------------------------------------------------------------
# URLs and links / URL ja linkit
# ---------------------------------------------------------------------------


def canonical_job_url(url: str) -> str:
    """Stable key: no query string, domain always present. / Yhtenäinen avain: ilman queryä, domain aina täynnä."""
    s = str(url).strip()
    if not s or s.lower() == "nan":
        return ""
    s = s.split("?", 1)[0].rstrip("/")
    if s and not s.startswith("http"):
        s = f"{BASE_DOMAIN.rstrip('/')}/{s.lstrip('/')}"
    return s


def is_job_posting_href(href: str) -> bool:
    """True if href is a single job posting, not the listing page. / Tosi, jos href on yksittäinen työilmoitus, ei listasivu."""
    if not href or RE_LISTING_PAGE.match(href):
        return False
    path = (href or "").split("?", 1)[0].rstrip("/")
    if re.search(r"/avoimet-tyopaikat/?$", path):
        return False
    if RE_JOB_PATH.search(href):
        return True
    m = re.search(r"/avoimet-tyopaikat/([^/?]+)", path, re.I)
    if m:
        seg = m.group(1)
        if len(seg) >= 8 and seg.lower() not in ("fi", "sv", "en"):
            return True
    return False


# ---------------------------------------------------------------------------
# Listing: cards / Listaus: kortit
# ---------------------------------------------------------------------------


def extract_yritys_from_listing_card(loc: Any, title_hint: str = "") -> str:
    """Company line 'Name | Julkaistu …' by walking up from the job link. / Rivi 'Yritys | Julkaistu …' — kävellään ylös linkistä."""
    hint = (title_hint or "").strip()
    try:
        raw = loc.evaluate(
            """(el, titleHint) => {
                const hint = (titleHint || '').toString().trim();
                const tryLine = (line) => {
                    const s = (line || '').trim();
                    const bar = s.indexOf('|');
                    if (bar < 0) return '';
                    if (!/Julkaistu/i.test(s.slice(bar + 1))) return '';
                    let left = s.slice(0, bar).trim();
                    if (!left || left.length > 200) return '';
                    if (hint && left.startsWith(hint)) {
                        left = left.slice(hint.length).trim();
                        left = left.replace(/^[,|\\s\\u00a0–-]+/, '').trim();
                    }
                    return left;
                };
                let n = el;
                for (let d = 0; d < 14 && n; d++) {
                    const rawText = n.innerText || '';
                    const lines = rawText.split(/\\r?\\n/).map(t => t.trim())
                        .filter(Boolean);
                    for (const line of lines) {
                        const got = tryLine(line);
                        if (got) return got;
                    }
                    const oneLine = rawText.replace(/\\s+/g, ' ').trim();
                    const got = tryLine(oneLine);
                    if (got) return got;
                    n = n.parentElement;
                }
                return '';
            }""",
            hint,
        )
    except Exception:
        return ""
    s = (raw or "").strip()
    return s[:500] if s else ""


def fetch_listing_page(page: Page, page_index: int, aggressive: bool = False) -> list[dict]:
    """One listing page: Linkki, Tehtävänimike, Yritys (card row). / Yhden sivun kortit: Linkki, Tehtävänimike, Yritys (listausrivi)."""
    url = LISTING_URL.format(p=page_index)
    for attempt in range(2):
        try:
            # "load" avoids networkidle hangs / "load" välttää networkidle-jäätymiset
            page.goto(url, wait_until="load", timeout=PAGE_GOTO_TIMEOUT_MS)
            break
        except Exception as e:
            if attempt == 0:
                print(f"  Yritetään uudelleen (sivu {page_index + 1})...", flush=True)
            else:
                print(f"Virhe sivun latauksessa (sivu {page_index + 1}): {e}", flush=True)
                return []

    wait_after = 4000 if aggressive else 2500
    scroll_steps = 25 if aggressive else 15
    step_wait = 600 if aggressive else 400
    wait_bottom = 4000 if aggressive else 2500
    try:
        page.wait_for_selector(
            "a[href*='/avoimet-tyopaikat/']",
            timeout=LISTING_SELECTOR_TIMEOUT_MS,
        )
        page.wait_for_timeout(wait_after)
        for _ in range(2):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)
        for step in range(scroll_steps):
            page.evaluate(f"window.scrollTo(0, {800 * (step + 1)})")
            page.wait_for_timeout(step_wait)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_bottom)
    except Exception:
        pass

    results: list[dict] = []
    seen: set[str] = set()
    for loc in page.locator("a[href*='/avoimet-tyopaikat/']").all():
        try:
            href = loc.get_attribute("href")
            if not is_job_posting_href(href):
                continue
            url_key = canonical_job_url(href or "")
            if not url_key:
                continue
            if url_key in seen:
                continue
            seen.add(url_key)

            title = (loc.inner_text() or "").strip()
            if not title or len(title) > 500:
                title = (loc.get_attribute("title") or "").strip()
            title = title or "-"

            yritys = extract_yritys_from_listing_card(loc, title_hint=title)
            yritys = clean_company_name(yritys)
            if looks_like_location(yritys):
                yritys = ""

            results.append(
                {"Linkki": url_key, "Tehtävänimike": title, "Yritys": yritys}
            )
        except Exception:
            continue

    if not results:
        n = len(page.locator("a[href*='/avoimet-tyopaikat/']").all())
        if n:
            print(f"  (Huom: {n} linkkiä, mikään ei täsmännyt.)", flush=True)
    return results


def fetch_all_listings(page: Page) -> list[dict]:
    """All paginated listing pages in order. / Kaikki sivut peräkkäin."""
    all_jobs: list[dict] = []
    page_num = 0
    same_page_fail = 0
    max_empty_retries = 4

    while True:
        print(f"Haetaan sivua {page_num + 1}...", flush=True)
        use_aggressive = page_num == 7
        batch = fetch_listing_page(page, page_num, aggressive=use_aggressive)
        expected = PAGE_SIZE

        if batch and len(batch) < expected:
            print(
                f"  Sivu {page_num + 1}: vain {len(batch)} korttia "
                f"(odotus {expected}), haetaan uudelleen...",
                flush=True,
            )
            batch2 = fetch_listing_page(page, page_num, aggressive=True)
            if len(batch2) > len(batch):
                batch = batch2
                print(f"  Uudelleenhaulla {len(batch)} korttia.", flush=True)

        if not batch:
            same_page_fail += 1
            if same_page_fail >= max_empty_retries:
                print(
                    f"  Lopetetaan (sivu {page_num + 1} tyhjä {same_page_fail} kertaa).",
                    flush=True,
                )
                break
            if len(all_jobs) % PAGE_SIZE == 0 and len(all_jobs) >= PAGE_SIZE:
                max_empty_retries = 6
            print(
                f"  Tyhjä sivu, yritetään uudelleen "
                f"({same_page_fail}/{max_empty_retries})...",
                flush=True,
            )
            page.wait_for_timeout(4000)
            continue

        same_page_fail = 0
        max_empty_retries = 4
        known = {j["Linkki"] for j in all_jobs}
        for job in batch:
            if job["Linkki"] not in known:
                known.add(job["Linkki"])
                all_jobs.append(job)

        print(f"Löytyi {len(batch)} ilmoitusta (yhteensä {len(all_jobs)}).", flush=True)
        if len(batch) < PAGE_SIZE:
            break
        page_num += 1

    return all_jobs


# ---------------------------------------------------------------------------
# Job page: Yritys only (no full card parse) / Ilmoitussivu: vain Yritys
# ---------------------------------------------------------------------------


def clean_company_name(value: str) -> str:
    """Return plausible company string or empty. / Palauta kelvollinen yritysnimi tai tyhjä."""
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
    """True if value looks like a place, not a company. / Tosi, jos arvo näyttää sijainnilta, ei yritykseltä."""
    s = (value or "").strip()
    if not s:
        return False
    low = s.lower()
    if any(tok in low for tok in LOCATION_SUBSTRINGS):
        return True
    return " " not in s and len(s) <= 14


def company_from_title_fallback(title: str) -> str:
    """Last title segment after comma only if legal suffix (Oy, …). / Viimeinen pilkkuerotettu vain jos Oy-tms."""
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
    """JSON-LD hiringOrganization, then Yritys label before location block. / JSON-LD, sitten Yritys-teksti ennen sijaintia."""
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
    """Open job URL; Yritys from JSON-LD / page label, else title suffix. / Avaa sivu; Yritys ilman täyttä korttiparsintaa."""
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
# Excel read/write / Excel (luku ja tallennus)
# ---------------------------------------------------------------------------


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure Linkki, Tehtävänimike, Yritys exist. / Varmista nämä sarakkeet."""
    for col in DATA_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def extract_urls_and_titles_from_excel(path: Path, n_rows: int) -> tuple[list[str], list[str]]:
    """URLs and display text from Tehtävänimike hyperlinks/HYPERLINK formulas. / URL ja näkyvä teksti hyperlinkistä tai HYPERLINK-kaavasta."""
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
    """DataFrame = only jobs on site; returns (df, added, removed_count, removed_links). / Taulukko = vain sivulla olevat ilmoitukset; palauttaa (df, uusia, poistettuja, poistetut linkit)."""
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
    for i in df.index:
        y = link_to_yritys.get(canonical_job_url(str(df.at[i, "Linkki"])), "")
        if y:
            df.at[i, "Yritys"] = y

    order = {canonical_job_url(j["Linkki"]): i for i, j in enumerate(jobs_from_web)}
    df["_ord"] = df["Linkki"].astype(str).apply(
        lambda x: order.get(canonical_job_url(x), 999)
    )
    df = df.sort_values("_ord").drop(columns=["_ord"])
    df.reset_index(drop=True, inplace=True)
    return df, len(new_rows), len(removed_links), removed_links


def row_has_valid_yritys(df: pd.DataFrame, i: int) -> bool:
    """True if Yritys is non-empty and not location-like. / Tosi, jos Yritys on täytetty eikä näytä sijainnilta."""
    if "Yritys" not in df.columns:
        return False
    v = df.at[i, "Yritys"]
    if v is None or not str(v).strip():
        return False
    return not looks_like_location(str(v))


def get_url_from_hyperlink_cell(cell) -> Optional[str]:
    """URL from cell hyperlink or HYPERLINK formula. / URL hyperlinkistä tai HYPERLINK-kaavasta."""
    h = getattr(cell, "hyperlink", None)
    if h:
        return getattr(h, "target", None) or getattr(h, "location", None)
    if isinstance(cell.value, str) and cell.value.startswith("=HYPERLINK("):
        m = re.search(r'=HYPERLINK\s*\(\s*"((?:[^"]|"")+)"', cell.value)
        if m:
            return m.group(1).replace('""', '"')
    return None


def find_title_column_index(ws) -> int:
    """1-based column index of Tehtävänimike header. / Tehtävänimike-sarakkeen indeksi (1-pohjainen)."""
    for c in range(1, ws.max_column + 1):
        if ws.cell(row=1, column=c).value == "Tehtävänimike":
            return c
    return 1


def save_workbook_atomic(wb, path: Path) -> None:
    """Save to temp file then os.replace (lower corruption risk). / Tallenna tempiin, sitten os.replace (vähentää korruptioriskiä)."""
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
    """Update Excel rows, hyperlinks, filter. Call often during run. / Päivitä rivit, hyperlinkit, suodatin. Kutsu usein kesken ajon."""
    df = ensure_columns(df)
    display_cols = ["Tehtävänimike", "Yritys"]

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
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"

    save_workbook_atomic(wb, EXCEL_PATH)
    print(f"Tallennettu: {last_data_row - 1} riviä.", flush=True)


def apply_hyperlinks_new_file(path: Path, urls: list[str]) -> None:
    """Set Tehtävänimike hyperlinks after creating a new workbook. / Aseta hyperlinkit uuden tiedoston jälkeen."""
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
    """Write HYPERLINK formula to cell. / Kirjoita HYPERLINK-kaava soluun."""
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
    """Refresh hyperlinks from df Linkki + Tehtävänimike. / Päivitä hyperlinkit df:n mukaan."""
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


def fill_card_details(page: Page, df: pd.DataFrame) -> None:
    """Fetch Yritys from job page only for incomplete rows. / Hae Yritys vain riveille, joilla se puuttuu tai on huono."""
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

        print(f"Haetaan kortti {i + 1}/{n}...", flush=True)
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
            print(f"Kortti {i + 1}/{n} ohitettu: {e}", flush=True)

    if skipped:
        print(f"Ohitettiin {skipped} valmiiksi täytettyä korttia.", flush=True)


# ---------------------------------------------------------------------------
# Entry point / Käynnistys
# ---------------------------------------------------------------------------


def main() -> None:
    """Run listing sync, save, then fill missing Yritys. / Listaa, synkkaa, tallenna, täytä puuttuva Yritys."""
    print("Työmarkkinatori -synkronointi alkaa.", flush=True)
    df: Optional[pd.DataFrame] = None

    try:
        # Playwright owns browser lifecycle (close on exit). / Selain suljetaan aina poistuttaessa.
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
                    jobs = fetch_all_listings(page)
                    if not jobs:
                        print("Verkosta ei löytynyt ilmoituksia.", flush=True)
                        return

                    print(f"Verkosta yhteensä {len(jobs)} ilmoitusta.", flush=True)
                    df, added, removed_n, _ = sync_dataframe(jobs)
                    print(
                        f"Synkronointi: +{added} uutta, -{removed_n} poistunutta.",
                        flush=True,
                    )
                    save_excel(df)

                    print("Haetaan Yritys-tiedot...", flush=True)
                    fill_card_details(page, df)
                    save_excel(df)
                    print(
                        "Synkronointi valmis. Tehtävänimike-sarake = hyperlink.",
                        flush=True,
                    )
                finally:
                    context.close()
            finally:
                browser.close()

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
