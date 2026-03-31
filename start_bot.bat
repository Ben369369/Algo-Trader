@echo off
cd /d "C:\Users\benon\OneDrive - Princeton University\Desktop\tradingbot"

:: Kill any existing bot instance
for /f "tokens=1" %%i in ('wmic process where "commandline like '%%main.py%%'" get processid 2^>nul ^| findstr /r "[0-9]"') do (
    taskkill /pid %%i /f >nul 2>&1
)

:: Start the bot in a minimized window
start "TradingBot" /min venv\Scripts\python.exe main.py
