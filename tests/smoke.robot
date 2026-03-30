*** Settings ***
Library    Process
Library    OperatingSystem

*** Variables ***
${SCRIPT}    ${CURDIR}/../jobmarket-scraper.py
${XLSX}      ${CURDIR}/../tyomarkkinatori_jobs.xlsx

*** Test Cases ***
Scraper Runs And Creates Excel
    ${result}=    Run Process    python    ${SCRIPT}    shell=False    timeout=1800s
    Should Be Equal As Integers    ${result.rc}    0
    Should Contain    ${result.stdout}    Työmarkkinatori -synkronointi alkaa.
    File Should Exist    ${XLSX}