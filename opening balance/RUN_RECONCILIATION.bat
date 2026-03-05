@echo off
chcp 65001 > nul
echo.
echo ============================================================
echo   Tally PDF vs Zoho Excel -- Full Reconciliation Suite
echo ============================================================
echo.

echo [STEP 1] Generating Excel Reconciliation Report...
python -X utf8 reconcile_pdf_vs_zoho.py
echo.

echo [STEP 2] Updating Frontend Dashboard JSON...
python -X utf8 export_frontend_json.py
echo.

echo ============================================================
echo   Done! Opening latest Excel report...
echo ============================================================
for /f "delims=" %%f in ('dir /b /od Reconciliation_Report_*.xlsx 2^>nul') do set LATEST=%%f
if defined LATEST (
    echo   Report: %LATEST%
    start "" "%LATEST%"
) else (
    echo   No Excel report found.
)
echo.
echo   Frontend: Open index.html in your browser to see the dashboard.
echo.
pause
