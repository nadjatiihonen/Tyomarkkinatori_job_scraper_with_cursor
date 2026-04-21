# tyomarkkinatori_job_scraper

Collects Tyomarkkinatori data and updates an existing Excel workbook with multiple tabs.
Kerää Työmarkkinatorin dataa ja päivittää olemassa olevan Excel-työkirjan useille välilehdille.

## What the script updates / Mitä skripti päivittää

- `IT` tab from:
  - `https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat?in=25&or=CLOSING`
- `Kirjanpito` tab from:
  - `https://tyomarkkinatori.fi/henkiloasiakkaat/avoimet-tyopaikat?q=kirjanpito&or=CLOSING`
- `koulutus` tab from:
  - `https://tyomarkkinatori.fi/henkiloasiakkaat/koulutukset-ja-palvelut?q=rekry&m=0&y=0&pa=1&eo=3&s=All&rs=All&re`

### Columns / Sarakkeet

- `IT` and `Kirjanpito`:
  - `Tehtävänimike` (hyperlink)
  - `Yritys`
  - `Työsuhde`
  - `Työaika`
- `koulutus`:
  - `Ohjelma` (hyperlink to training card)
  - `Järjestäjä`
  - `Sijainti`
  - `Kesto`
  - `Haku paättyy`

## Run / Aja

- `python3 job_collector.py`

## Technologies / Teknologiat

- Python 3.9+
- requests
- Playwright (browser automation / selainautomaatio)
- pandas (table processing / taulukon käsittely)
- openpyxl (Excel read/write / Excel luku-kirjoitus)
