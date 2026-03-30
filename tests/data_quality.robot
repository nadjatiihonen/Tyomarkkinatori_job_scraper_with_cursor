*** Settings ***
Library    Process

*** Variables ***
${CHECK_SCRIPT}    ${CURDIR}/check_data_quality.py

*** Test Cases ***
Excel Data Quality Is Acceptable
    ${result}=    Run Process    python    ${CHECK_SCRIPT}    shell=False
    Should Be Equal As Integers    ${result.rc}    0
