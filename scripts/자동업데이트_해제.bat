@echo off
chcp 65001 >nul
schtasks /Delete /TN "MusinsaDailyLookup" /F
if errorlevel 1 (
  echo 예약 작업을 찾지 못했거나 삭제하지 못했습니다.
) else (
  echo 자동 업데이트 예약을 삭제했습니다.
)
pause
