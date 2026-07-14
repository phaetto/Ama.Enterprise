@echo off
REM Evaluates locally installed Azurite and starts it natively without blocking the MSBuild process.

REM Check if the Table Storage port (10002) is already in use
netstat -ano | find "127.0.0.1:10002" >nul
if %errorlevel%==0 (
    echo Azurite is already running.
    exit /b 0
)

echo Azurite not running. Attempting to start natively...

REM 1. Check if Azurite was installed globally via npm (Node.js)
where azurite >nul 2>nul
if %errorlevel%==0 (
    powershell -NoProfile -Command "Start-Process cmd -ArgumentList '/c azurite --loose --location .azurite'"
    exit /b 0
)

REM 2. Fallback to Visual Studio 2022 bundled Azurite installations
set "VSAZ_ENT=%ProgramFiles%\Microsoft Visual Studio\2022\Enterprise\Common7\IDE\Extensions\Microsoft\Azure Storage Emulator\azurite.exe"
set "VSAZ_PRO=%ProgramFiles%\Microsoft Visual Studio\2022\Professional\Common7\IDE\Extensions\Microsoft\Azure Storage Emulator\azurite.exe"
set "VSAZ_COM=%ProgramFiles%\Microsoft Visual Studio\2022\Community\Common7\IDE\Extensions\Microsoft\Azure Storage Emulator\azurite.exe"

if exist "%VSAZ_ENT%" (
    powershell -NoProfile -Command "Start-Process '%VSAZ_ENT%' -ArgumentList '--loose --location .azurite'"
    exit /b 0
)
if exist "%VSAZ_PRO%" (
    powershell -NoProfile -Command "Start-Process '%VSAZ_PRO%' -ArgumentList '--loose --location .azurite'"
    exit /b 0
)
if exist "%VSAZ_COM%" (
    powershell -NoProfile -Command "Start-Process '%VSAZ_COM%' -ArgumentList '--loose --location .azurite'"
    exit /b 0
)

echo [Warning] Azurite was not found. Please install it via 'npm install -g azurite'.
exit /b 0