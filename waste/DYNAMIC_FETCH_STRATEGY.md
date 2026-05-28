# Multi-Company Migration Strategy (Code Word: DYNAMIC FETCH)

This document contains the core architectural strategy for Phase 3 of the Tally to Zoho Books Migration Tool. 
Whenever you need the AI to remember the multi-client dynamic setup, use the code word: **"DYNAMIC FETCH"** and instruct the AI to read this file.

## 1. Dynamic Database Splitting (Multi-Tenant)
- **Problem:** Migration teams working with multiple clients simultaneously cannot use a single `tally_data.db` file. Mix-ups will occur.
- **Solution:** Dynamically generate SQLite databases for each company.
- **Implementation:** 
  - Instead of a hardcoded `tally_data.db`, the app will use sessions to select active projects (e.g., `db_clientA.db`, `db_clientB.db`).
  - A "Project Selection" screen will be built on the front end to switch contexts.

## 2. Dynamic Voucher Type Mapping
- **Problem:** Tally universal tags (`<DATE>`, `<VOUCHERNUMBER>`) never change between ERP 9 and Prime. However, clients create custom voucher names (e.g., `Audit JV2` instead of `Journal`).
- **Solution:** Do not hard-code strict `VOUCHERTYPENAME` matching in the final product.
- **Implementation:** 
  - Build a mapping UI (similar to the Chart of Accounts mapping). 
  - The migration team will map Tally's custom voucher names to Zoho Books' standard modules.

## 3. Extract Everything, Format Later
- **Problem:** Tally configurations differ drastically between retail, manufacturing, and service clients.
- **Solution:** The Python backend must extract the complete XML raw data as a massive JSON object to the local database *before* filtering.
- **Implementation:** Python scripts will check for the existence of optional fields (like `cost_center_allocations`) dynamically at insertion time. If it exists in the JSON, sync it; if not, safely ignore it without crashing.

## Summary Rule for AI:
*Never hardcode specific customer names or limit fields strictly; always design Python scripts to look for universal Tally XML schemas. Allow the Front-End to handle custom edge cases via user UI mapping.*
