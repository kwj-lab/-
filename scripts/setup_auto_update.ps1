$ErrorActionPreference = 'Stop'
$taskName = 'MusinsaDailyLookup'
$baseDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

# When distributed in package, script is next to exe. When run from repo/scripts, exe may be in parent/dist.
$candidates = @(
    (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'musinsa_tracker.exe'),
    (Join-Path $baseDir 'musinsa_tracker.exe'),
    (Join-Path $baseDir 'dist\musinsa_tracker.exe')
)
$exe = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $exe) {
    Write-Host ''
    Write-Host 'ERROR: musinsa_tracker.exe 파일을 찾을 수 없습니다.' -ForegroundColor Red
    Write-Host 'GitHub Actions에서 받은 ZIP을 압축 해제한 폴더에서 실행하세요.'
    exit 1
}

$workingDir = Split-Path -Parent $exe
Write-Host ''
Write-Host '무신사 자동조회 스케줄 설정' -ForegroundColor Cyan
Write-Host '--------------------------------'
Write-Host '등록 브랜드의 신규 상품번호를 먼저 추가한 뒤 전체 상품을 매일 자동 조회합니다.'
Write-Host ''

$runTime = Read-Host '매일 실행할 시간을 HH:mm 형식으로 입력하세요 (예: 09:30, 기본 09:00)'
if ([string]::IsNullOrWhiteSpace($runTime)) { $runTime = '09:00' }

$parsed = $null
if (-not [TimeSpan]::TryParseExact($runTime, 'hh\:mm', $null, [ref]$parsed)) {
    Write-Host '시간 형식이 올바르지 않습니다. 예: 09:30 또는 23:00' -ForegroundColor Red
    exit 2
}

$at = [datetime]::Today.Add($parsed)
$action = New-ScheduledTaskAction -Execute $exe -Argument '--auto' -WorkingDirectory $workingDir
$trigger = New-ScheduledTaskTrigger -Daily -At $at
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description '무신사 상품 자동 일일 조회' -Force | Out-Null

Write-Host ''
Write-Host "설정 완료: 매일 $runTime 자동 조회" -ForegroundColor Green
Write-Host "작업 이름: $taskName"
Write-Host '프로그램에서 브랜드/자동조회 목록을 먼저 저장해 두세요.' -ForegroundColor Yellow
Write-Host ''
Read-Host 'Enter 키를 누르면 종료합니다'
