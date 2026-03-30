*** Settings ***
Library    Process

*** Variables ***
${CHECK_SCRIPT}    ${CURDIR}/check_schema.py

*** Test Cases ***
Excel Schema Is Correct
    ${result}=    Run Process    python    ${CHECK_SCRIPT}    shell=False
    Should Be Equal As Integers    ${result.rc}    0
