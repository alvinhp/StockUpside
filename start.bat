@echo off
echo.
echo   ^▲  StockUpside.io
echo   ---------------------------------

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo   x  Python not found. Install Python 3.9+ from python.org
    pause
    exit /b 1
)

echo   -^>  Checking Python dependencies...
python -c "import flask" >nul 2>&1
if %errorlevel% neq 0 (
    pip install flask --quiet
)

where tsc >nul 2>&1
if %errorlevel% equ 0 (
    echo   -^>  Compiling TypeScript...
    call tsc
    echo   OK  TypeScript compiled.
) else (
    echo   !   tsc not found - using pre-compiled public\main.js
)

echo   -^>  Starting server on http://localhost:5000
echo.
python server\app.py
pause
