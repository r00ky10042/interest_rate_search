@echo off
chcp 65001 > nul
echo 서버 시작 중... http://localhost:5000
"C:\Users\TM00000002\AppData\Local\Programs\Python\Python312\python.exe" "%~dp0app.py"
pause
