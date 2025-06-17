@echo off
start "Incantation Server" "C:\Program Files\Git\bin\bash.exe" -c "python main_server.py; echo 'Server stopped. Press any key to close...'; read -n 1"
pause