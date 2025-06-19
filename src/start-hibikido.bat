@echo off
start "Hibikido Server" cmd /k ^
"color 0D ^
& title Hibikido Server ^
& echo ============================================ ^
& echo           HIBIKIDO SERVER STARTING ^
& echo ============================================ ^
& echo. ^
& hibikido-server ^
& if errorlevel 1 (color 0C ^
& echo. ^
& echo ============================================ ^
& echo         SERVER EXITED WITH ERROR ^
& echo ============================================ ^
& echo. ^
& pause ^
& exit) else (color 0D ^
& echo. ^
& echo ============================================ ^
& echo         SERVER STOPPED NORMALLY ^
& echo ============================================ ^
& echo. ^
& pause ^
& exit) ^
& exit"