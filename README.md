# tyomarkkinatori_job_scraper

Collects open jobs and training listings from [Työmarkkinatori](https://tyomarkkinatori.fi) and syncs them into `tyomarkkinatori_jobs.xlsx` (one tab per search).

Kerää Työmarkkinatorin avoimet työpaikat ja koulutukset Excel-tiedostoon `tyomarkkinatori_jobs.xlsx` (yksi välilehti per haku).

## What the script updates / Mitä skripti päivittää

| Excel tab | Source listing |
|-----------|----------------|
| `sihteeri` | [Avoimet työpaikat — haku: sihteeri](https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat?q=sihteeri) |
| `Kirjanpito` | [Avoimet työpaikat — haku: kirjanpito](https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat?q=kirjanpito&or=CLOSING) |
| `koulutus` | [Koulutukset ja palvelut — haku: rekry](https://tyomarkkinatori.fi/henkiloasiakkaat/koulutukset-ja-palvelut?q=rekry&m=0&y=0&pa=1&eo=3&s=All&rs=All&re) |

### Columns / Sarakkeet

**`sihteeri` and `Kirjanpito`:**

- `Tehtävänimike` — job title (hyperlink to the posting)
- `Yritys` — employer (from API; Playwright fills gaps if needed)
- `Työsuhde` — contract type
- `Työaika` — working hours
- `Julkaistu` — publication date

**`koulutus`:**

- `Ohjelma` — programme name (hyperlink)
- `Järjestäjä` — provider
- `Sijainti` — location
- `Kesto` — duration
- `Haku paättyy` — application deadline
- `Julkaistu` — publication date

## Run / Aja

```bash
python3 job_collector.py
```

Dependencies: see `requirements.txt`. After install, run once: `playwright install chromium`.

Riippuvuudet: `requirements.txt`. Asenna selain kerran: `playwright install chromium`.

## Technologies / Teknologiat

- Python 3.9+
- requests — Työmarkkinatori JSON API
- pandas — table sync
- openpyxl — Excel read/write
- Playwright — optional browser pass for missing `Yritys`
