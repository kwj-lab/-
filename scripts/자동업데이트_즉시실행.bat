@echo off
chcp 65001 >nul
cd /d "%~dp0"
if exist "musinsa_tracker.exe" (
  musinsa_tracker.exe --auto
) else if exist "..\musinsa_tracker.exe" (
  "..\musinsa_tracker.exe" --auto
) else (
  echo musinsa_tracker.exe 파일을 찾을 수 없습니다.
)
pause
