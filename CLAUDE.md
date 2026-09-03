# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

QFieldCloud에 올라온 재난안전 피해시설물 현장조사 데이터(GPKG)를 주기적으로 스캔하여 PostGIS(`qfield` 스키마)로 실시간 동기화하는 파이썬 배치 엔진이다. 별도의 웹 프레임워크나 API 서버 없이, 무한 루프(`main()`)로 동작하는 단일 워커 프로세스로 구성된다.

핵심 스크립트는 [qfield_data_sync.py](qfield_data_sync.py) 하나뿐이며, 부가 모듈로 음성(m4a/wav) STT 변환을 담당하는 [disaster2convert.py](disaster2convert.py)가 있다.

## 실행/개발 명령

이 저장소에는 빌드 시스템, 린터, 테스트 스위트가 없다. 순수 파이썬 스크립트로 직접 실행/검증한다.

```bash
# 의존성 설치 (venv 사용 권장)
pip install -r requirements.txt

# 로컬에서 동기화 엔진 실행 (FLASK_ENV=local이면 D:/work/qfield에 다운로드)
python qfield_data_sync.py

# 문법 검증만 빠르게 확인 (실제 DB/QFieldCloud 접속 없이)
python -m py_compile qfield_data_sync.py

# Docker 이미지 빌드/실행 (배포용, GDAL/psycopg2/pydub용 OS 패키지 포함)
docker build -t sj-qfieldsync .
docker run -e FLASK_ENV=production -v /host/qfield:/app/webfiles/qfield sj-qfieldsync
```

`FLASK_ENV` 환경변수로 로컬(`local`, 기본값)과 운영(`/app/webfiles/qfield`) 다운로드 경로가 갈린다([qfield_data_sync.py:68-70](qfield_data_sync.py#L68-L70)).

## 아키텍처

### 전체 흐름 (main 루프, 30초 주기)

`main()` → `get_all_projects_from_db()`로 QFieldCloud 메타 DB의 전체 프로젝트 목록 조회 → 프로젝트별 `get_latest_job_id()`(마지막 `delta_apply` job 완료 시각)를 캐시와 비교해 변경분만 감지 → 변경된 프로젝트만 `sync_single_project()` 실행 → 삭제된 프로젝트는 `cleanup_deleted_projects()`로 정리 → 테이블 추가/삭제가 있었으면 `update_facility_total_view()`로 통합 뷰 재생성.

### 두 개의 서로 다른 DB를 다룬다

- `QFC_DB` — QFieldCloud 자체의 메타데이터 DB(프로젝트/유저/작업이력). `get_qfc_conn()`으로 접속하며 **읽기 위주**(권한 부여 INSERT 제외).
- `DATA_DB` — 최종 데이터 적재 대상 PostGIS DB(`qfield` 스키마). `get_data_conn()`으로 접속하며 실제 테이블 생성/UPSERT/DROP이 모두 이곳에서 일어난다.

두 커넥션을 혼동하지 않도록 주의한다. 접속 정보는 현재 [qfield_data_sync.py:44-65](qfield_data_sync.py#L44-L65)에 평문으로 하드코딩되어 있다(코드 수정 시 실수로 노출/커밋하지 않도록 유의).

### 테이블 명명 규칙과 동적 스키마

QFieldCloud 프로젝트마다 GPKG 레이어를 개별 물리 테이블로 만든다. 테이블명은 `{owner}_{project_id[:13]}_{gpkg파일명}_{레이어명}` 형태로 슬러그화되어 생성된다(`process_gpkg_to_db`, `_slugify_table_part`). 컬럼은 GPKG 레이어의 속성을 그대로 TEXT/TIMESTAMP로 반영하며 테이블이 이미 있으면 `ALTER TABLE ADD COLUMN IF NOT EXISTS`로 보강한다 — 즉 스키마가 고정되어 있지 않고 매 동기화마다 동적으로 진화한다.

`qfield.qfield_info` 테이블의 `column_list`(qfield_type='facility')에 등록된 컬럼과 하나도 겹치지 않는 레이어는 무관한 레이어로 간주되어 스킵된다(`get_target_columns`, `process_gpkg_to_db`의 intersection 체크). 새 필드 조사 양식을 반영하려면 이 `qfield_info.column_list`를 먼저 갱신해야 한다.

### UPSERT / 소프트 삭제 패턴

모든 물리 테이블은 `own_id`(BIGSERIAL PK), `src_key`(GPKG 원본 fid), `use_yn`(y/n)을 갖는다. `use_yn='y'`인 행에 대해서만 `src_key` 유니크 인덱스가 걸려 있어 "현재 활성 행"과 "과거 이력"이 공존할 수 있다(`ON CONFLICT (src_key) WHERE use_yn='y'`). 소스에서 사라진 `src_key`는 물리 삭제하지 않고 `use_yn='n'`으로만 바꾼다([qfield_data_sync.py:364-371](qfield_data_sync.py#L364-L371)). 이 소프트 삭제 관례를 깨는 변경(예: 실제 DELETE)은 하지 않는다.

### 음성/메모 필드 자동 STT 변환

컬럼명에 `record`/`audio`/`memo`가 포함되면 `{컬럼명}_txt` 컬럼이 자동 생성되고, 원본 파일 경로를 `disaster2convert.read_audio()`로 STT 변환한 텍스트가 저장된다(`save_gdf_direct`의 `final_cols` 구성부). `disaster2convert` 임포트 실패 시에는 STT 없이 빈 문자열로 채워지고 전체 동기화는 계속 진행된다.

### 프로젝트 삭제 처리와 `total_seq`/`total_id` 불변성

QFieldCloud에서 프로젝트가 삭제되면 `cleanup_deleted_projects()`가 테이블명에 박힌 short_id로 현재 존재하는 프로젝트와 매칭해 사라진 프로젝트의 테이블을 찾는다. 이후 `archive_and_drop_table()`이:
1. 모든 행을 `facility_deleted_archive`로 이관(커스텀 속성은 JSONB `attrs`로 통합)
2. 이관 시점에 `orig_total_seq`/`orig_total_id`를 고정 계산해 저장
3. 원본 테이블을 DROP

이렇게 하는 이유는 `facility_total_view`(통합 뷰)의 `total_seq = table_idx * 10000000 + own_id`, `total_id = 'FACIL_T{idx}_{own_id}'` 값이 테이블 삭제 전후로 절대 바뀌지 않아야 하기 때문이다. `table_idx`는 `table_seq_registry`에 영구 고정되며 재사용되지 않는다(`ensure_table_idx_registry`). **이 불변성(삭제 전/후 ID 안정성)은 이 프로젝트의 핵심 설계 의도이므로, 관련 코드를 수정할 때는 반드시 유지해야 한다.**

### `facility_total_view`

`update_facility_total_view()`가 `qfield` 스키마의 모든 물리 테이블 + `facility_deleted_archive`를 UNION ALL로 묶어 재생성하는 뷰다. 컬럼 구조가 테이블마다 다르므로 전체 테이블의 컬럼 합집합을 구해 없는 컬럼은 `NULL`로 채워 맞춘다. 테이블이 추가/삭제될 때마다(`cycle_updated or deleted_occurred`) 매번 `DROP VIEW` 후 재생성한다.

## 참고

### 파일 구성 메모

- [qfield_data_sync.py](qfield_data_sync.py) — 현재 운영 중인 메인 스크립트.
- [qfield_data_sync_20260828.py](qfield_data_sync_20260828.py) — 날짜가 붙은 이전 버전 스냅샷(수동 백업)이다. 삭제 처리가 소프트 삭제(`use_yn='n')`만 하던 구버전 로직이며, 현재는 아카이브+DROP 방식으로 대체되었다([qfield_data_sync.py:9-12](qfield_data_sync.py#L9-L12) 참고). 편집 대상이 아니다 — 혼동하지 말 것.
- [known issue/issue](known%20issue/issue) — 알려진 미해결 이슈 메모: "QGIS/QFieldCloud에서 프로젝트를 직접 삭제 시 테이블이 sync되지 않는 오류".
- [qgis_project/facility.qgs.qgz](qgis_project/facility.qgs.qgz) — 피해시설물 기본설정 QGIS 프로젝트 파일(바이너리, 편집 대상 아님).
- [disaster2convert.py](disaster2convert.py) — Google STT(`speech_recognition.recognize_google`, 한국어) 기반 m4a/wav → 텍스트 변환 모듈. 네트워크 의존(구글 API)이라 오프라인 환경에서는 항상 실패하고 빈 문자열을 반환한다.

### 에러 처리 관례

거의 모든 함수가 `try/except Exception`으로 개별 실패를 흡수하고 `log()`로 기록한 뒤 계속 진행한다(한 프로젝트/레이어 실패가 전체 루프를 멈추지 않도록). DB 함수는 `finally`에서 반드시 `conn.close()`를 호출하고, 쓰기 작업 실패 시 `conn.rollback()`을 먼저 호출하는 패턴을 따른다. 새 DB 함수를 추가할 때도 이 패턴(개별 try/except + finally close)을 유지한다.

### SQL 문자열 조합 시 주의

스키마/테이블/컬럼명은 파라미터 바인딩이 불가능해 f-string으로 직접 SQL에 삽입한다(`schema`, `table_name`, GPKG에서 유래한 컬럼명 등). 값(데이터)은 항상 `%s` 플레이스홀더로 바인딩한다. 새 컬럼/테이블명을 조합하는 코드를 추가할 때는 `_slugify_table_part()` 같은 화이트리스트 정규화를 거치지 않은 외부 입력을 SQL에 직접 넣지 않도록 주의한다.
