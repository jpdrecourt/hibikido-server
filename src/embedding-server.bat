@echo off
start "Embedding Server" "C:\Program Files\Git\bin\bash.exe" -c "python embedding-server.py; echo 'Server stopped. Press any key to close...'; read -n 1"
pause