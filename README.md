# 무신사 판매 추적기

무신사 상품번호를 기준으로 공개 웹/API에서 확인 가능한 값을 주기적으로 저장해 판매 흐름을 추적하는 Windows용 도구입니다.

## 주요 기능

- 상품번호 여러 개 일괄 조회
- `purchaseTotal` 누적 구매수 추적
- 누적 조회수, 정상가, 현재가, 할인율, 리뷰, 평점, 좋아요, 판매상태 조회
- `현재가 × 누적 구매수` 기준 단순 누적 GMV 계산
- 이전 조회 대비 구매수 증가량 및 증가 GMV 계산
- 추적 브랜드명을 저장하고, 무신사 검색 결과에서 동일한 `brandName`의 신규 `goodsNo`를 자동 추가
- Windows 작업 스케줄러를 이용한 매일 자동 업데이트
- 날짜별 스냅샷 및 일별 합계 CSV 저장

> `purchaseTotal`은 공개 API의 누적 구매수 필드입니다. 실제 출고수량·순매출·브랜드 정산금액과 동일하다고 단정할 수 없습니다. 단순 누적 GMV도 과거 할인/쿠폰/취소·반품을 반영하지 않습니다.

## Windows EXE 받는 방법

1. GitHub 저장소의 **Actions** 탭으로 이동합니다.
2. `Build Windows EXE` 워크플로를 엽니다.
3. 가장 최근 성공한 실행을 선택합니다.
4. 아래 **Artifacts**에서 `musinsa-tracker-windows`를 다운로드합니다.
5. ZIP 압축을 풀고 `musinsa_tracker.exe`를 실행합니다.

`main` 또는 `master`에 변경사항을 Push할 때마다 Windows EXE가 자동으로 다시 빌드됩니다.

## 프로그램 사용

### 1. 상품번호 직접 추적
상품번호 또는 상품 URL을 여러 줄로 붙여넣고 `일괄 조회`를 누릅니다.

예:

```text
7024843
7024844
https://www.musinsa.com/products/7024845
```

자동 추적하려면 입력 후 `자동조회 목록 저장`을 누릅니다.

### 2. 브랜드 신상품 자동 추가

`추적 브랜드` 칸에 무신사에 표시되는 브랜드명을 한 줄에 하나씩 넣습니다.

```text
수아레
라퍼지스토어
굿라이프웍스
```

`자동추가 브랜드 저장` → `지금 새 상품 찾기` 순서로 누르면, 검색 결과 중 `brandName`이 정확히 일치하는 신규 상품번호만 기존 watchlist에 추가합니다.

### 3. 매일 자동 실행

GitHub Actions에서 받은 ZIP의 `자동업데이트_설정.bat`을 실행하고 원하는 시간을 입력합니다.

예약 실행 시:

1. 브랜드 신상품 탐색
2. 신규 `goodsNo` watchlist 병합
3. 전체 상품 조회
4. CSV 이력 저장
5. 날짜별 snapshot/summary 저장

## 생성되는 파일

- `musinsa_watchlist.txt` — 자동 조회 상품번호
- `musinsa_brands.txt` — 신상품 자동 탐색 브랜드
- `musinsa_new_products.csv` — 신규 상품 최초 발견 기록
- `musinsa_bulk_history.csv` — 상품별 전체 조회 이력
- `musinsa_daily_summary.csv` — 일별 전체 합계
- `daily_snapshots/` — 실행 시점별 전체 상품 스냅샷
- `musinsa_auto_update.log` — 자동 실행 로그

이 데이터 파일들은 `.gitignore`에 포함되어 GitHub에 자동 업로드되지 않습니다.

## 개발 / 로컬 실행

Python 3.10+ 기준입니다. 별도 런타임 패키지 없이 표준 라이브러리만 사용합니다.

```bash
python musinsa_tracker.py
```

자동 실행 모드:

```bash
python musinsa_tracker.py --auto
```

## GitHub Actions

`.github/workflows/build-windows.yml`이 Windows runner에서 PyInstaller로 단일 EXE를 생성합니다.

버전 태그를 Push하면 GitHub Release에도 ZIP을 자동 첨부합니다.

```bash
git tag v0.1.0
git push origin v0.1.0
```

## 주의사항

이 프로그램은 무신사의 공개 웹 화면 및 공개적으로 접근 가능한 비공식 API 구조에 의존합니다. 무신사 측 API/페이지 구조가 바뀌면 일부 기능이 동작하지 않을 수 있습니다. 요청 빈도는 낮게 유지하도록 구성되어 있으며, 과도한 수집은 피하는 것이 좋습니다.
