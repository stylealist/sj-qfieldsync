# sj-qfieldsync — 현장조사 데이터 동기화 워커

> QFieldCloud에 올라온 **현장조사 데이터(GPKG)를 30초마다 감지해 PostGIS로 동기화**하는 파이썬 워커입니다.
> 모바일 현장조사와 웹 지도 서비스를 잇는 연결 고리이며, **조사 양식이 계속 바뀌는 환경에서 스키마를 어떻게 다룰지**가 이 프로젝트의 핵심 과제였습니다.

| | |
|---|---|
| **결과물** | https://sj-lab.co.kr/map/ 의 시설물 데이터 |
| **스택** | Python · GeoPandas/GDAL · psycopg2 · PostGIS · SpeechRecognition(STT) · pydub |
| **형태** | 웹 프레임워크 없이 무한 루프로 도는 단일 워커 프로세스 |

---

## 1. 데이터 흐름

```
[현장] infra-manage-app (QField 포크) — 시설물 점검·사진·음성 메모
        │ 업로드
[QFieldCloud] https://qfield.sj-lab.co.kr
        │ 변경 감지(30초) → GPKG 다운로드
[이 워커] 동적 테이블 생성/보강 → UPSERT → facility_total_view 재생성
        ▼
[PostGIS] qfield 스키마
        │
[백엔드] mapservice-rest ──▶ [지도 프론트]
```

---

## 2. 면접에서 봐주셨으면 하는 부분

### ① 변경분만 처리 — 전수 스캔을 피하는 감지 방식

매 주기 전체 프로젝트를 내려받으면 QFieldCloud와 DB에 모두 부담이 됩니다. QFieldCloud 메타 DB에서 프로젝트별 **마지막 `delta_apply` job 완료 시각**을 읽어 캐시와 비교하고, **바뀐 프로젝트만** 내려받아 처리합니다.

### ② 고정되지 않는 스키마를 다루는 방법

조사 양식은 현장 요구에 따라 계속 바뀝니다. 그래서 GPKG 레이어마다 물리 테이블을 만들되,

- 테이블명은 `{owner}_{project_id[:13]}_{gpkg}_{layer}`로 **슬러그화**해 충돌을 막고
- 컬럼은 레이어 속성을 그대로 반영하며, 기존 테이블에는 `ALTER TABLE ADD COLUMN IF NOT EXISTS`로 **보강**합니다
- `qfield_info.column_list`와 컬럼이 하나도 겹치지 않는 레이어는 **무관한 레이어로 보고 스킵**합니다(다른 프로젝트가 섞여 들어오는 것 방지)

즉 스키마가 **매 동기화마다 진화**하는 구조를 의도적으로 택했습니다.

### ③ ID 불변성 — 이 프로젝트의 가장 중요한 설계 제약

지도 서비스의 내업 기록은 시설물 `total_id`를 **논리 FK**로 참조합니다. 그런데 QFieldCloud에서 프로젝트를 삭제하면 테이블이 사라집니다. 이때 ID가 재계산되면 **기존 내업 기록이 엉뚱한 시설물을 가리키게** 됩니다.

그래서:

- `table_idx`를 `table_seq_registry`에 **영구 고정**하고 재사용하지 않습니다
- `total_seq = table_idx * 10_000_000 + own_id`, `total_id = 'FACIL_T{idx}_{own_id}'`
- 프로젝트 삭제 시 `archive_and_drop_table()`이 모든 행을 `facility_deleted_archive`로 옮기면서 **그 시점의 `orig_total_seq`/`orig_total_id`를 고정 저장**한 뒤 원본을 DROP

결과적으로 **테이블이 사라져도 ID는 절대 바뀌지 않습니다.**

### ④ 소프트 삭제로 이력 보존

모든 테이블은 `own_id`(PK)·`src_key`(GPKG 원본 fid)·`use_yn`을 갖고, **`use_yn='y'`인 행에만 유니크 인덱스**를 걸었습니다(`ON CONFLICT (src_key) WHERE use_yn='y'`). 덕분에 "현재 활성 행"과 "과거 이력"이 한 테이블에 공존하고, 소스에서 사라진 데이터도 물리 삭제 없이 `use_yn='n'`으로만 처리합니다.

### ⑤ 컬럼 구조가 제각각인 테이블을 하나의 뷰로

프로젝트마다 컬럼이 달라 그대로는 조회할 수 없습니다. `update_facility_total_view()`가 **전체 테이블의 컬럼 합집합**을 구해 없는 컬럼은 `NULL`로 채우고 `UNION ALL`로 묶어 `facility_total_view`를 재생성합니다(테이블 추가·삭제가 있을 때만). 백엔드는 이 뷰 하나만 보면 됩니다.

### ⑥ 음성 메모 자동 전사

컬럼명에 `record`/`audio`/`memo`가 있으면 `{컬럼명}_txt` 컬럼을 자동 생성하고 STT 결과를 저장합니다. **전사 실패가 동기화를 막지 않도록** 임포트·변환 실패 시 빈 문자열로 채우고 계속 진행합니다(네트워크 의존 기능을 배치의 필수 경로에 두지 않음).

### ⑦ 장애 격리

한 프로젝트·레이어의 실패가 전체 루프를 멈추면 안 되므로, 거의 모든 함수가 개별 `try/except`로 실패를 흡수하고 로그를 남긴 뒤 계속 진행합니다. DB 함수는 `finally`에서 `conn.close()`, 쓰기 실패 시 `rollback()`을 먼저 호출하는 패턴으로 통일했습니다.

---

## 3. 실행

```bash
pip install -r requirements.txt

python qfield_data_sync.py                # 동기화 엔진
python -m py_compile qfield_data_sync.py  # DB 접속 없이 문법만 검증
```

Docker:

```bash
docker build -t sj-qfieldsync .
docker run -e FLASK_ENV=production -v /host/qfield:/app/webfiles/qfield sj-qfieldsync
```

`FLASK_ENV`로 다운로드 경로가 갈립니다(`local` 기본 / 운영 `/app/webfiles/qfield`).

> ⚠️ **로컬에서 실행하면 실제 DB에 적재됩니다.** 검증 목적으로 함부로 돌리지 않습니다.

---

## 4. 두 개의 DB

| 커넥션 | 대상 | 용도 |
|---|---|---|
| `QFC_DB` | QFieldCloud 메타 DB | 프로젝트·유저·작업이력 **읽기 위주** |
| `DATA_DB` | PostGIS `qfield` 스키마 | 테이블 생성·UPSERT·DROP |

두 커넥션을 혼동하지 않도록 접근 함수를 분리했습니다.

---

## 5. 파일 구성

| 파일 | 설명 |
|---|---|
| `qfield_data_sync.py` | **운영 중인 메인 스크립트** |
| `qfield_data_sync_20260828.py` | 이전 버전 스냅샷(구버전 삭제 로직) — 편집 대상 아님 |
| `disaster2convert.py` | m4a/wav → 텍스트(STT) 변환 |
| `qgis_project/facility.qgs.qgz` | 피해시설물 기본설정 QGIS 프로젝트 |
| `known issue/issue` | 미해결 이슈 메모 |

---

## 6. 주의 · 한계

- **`qfield` 스키마에 앱용 테이블을 만들면 안 됩니다.** `cleanup_deleted_projects()`가 프로젝트 이름 패턴이 아닌 테이블을 "삭제된 프로젝트"로 보고 아카이브 후 DROP합니다(실제로 두 테이블이 삭제돼 `map` 스키마로 이전한 이력이 있습니다).
- 접속 정보가 스크립트에 평문으로 하드코딩돼 있습니다(환경변수·시크릿 이전이 과제).
- 테이블·컬럼명은 바인딩이 불가능해 f-string으로 조합하므로, 반드시 슬러그 정규화를 거칩니다. 값은 항상 `%s` 바인딩입니다.
- 테스트·린터가 없습니다. 검증은 `py_compile`과 실제 실행 로그에 의존합니다.

## 참고

- 전체 구조: 총괄 저장소 `mapservice-rest`의 `docs/system-architecture.md`
- 설계 의도 상세: 이 저장소의 `CLAUDE.md`
