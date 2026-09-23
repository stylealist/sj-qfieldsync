# sj-qfieldsync — 현장조사 데이터 실시간 동기화 워커 (ETL)

`sj-qfieldsync`는 QFieldCloud에 업로드되는 모바일 현장조사 데이터(GeoPackage)를 30초 주기로 감지하여 PostGIS 데이터베이스로 증분 동기화(Incremental ETL)하는 Python 기반 백그라운드 동기화 엔진입니다. 동적으로 변화하는 현장 조사 양식(Schema Evolution)을 흡수하고, 전체 시설물을 단일 뷰(`qfield.facility_total_view`)로 통합하여 백엔드 및 웹 지도에 제공합니다.

---

## 1. 서비스 역할 및 핵심 책임

- **증분 변경 감지(Change Data Capture)**: QFieldCloud 메타 DB의 작업 완료 이력(`delta_apply`)을 감시하여 최근 변경된 프로젝트만 선별적으로 다운로드 및 처리합니다.
- **스키마 진화(Schema Evolution) 수용**: 조사 항목 변경에 따라 GPKG 레이어의 속성이 추가되면 기존 PostGIS 테이블을 `ALTER TABLE ADD COLUMN`으로 무중단 자동 확장합니다.
- **영구 불변 고유 ID(Immutable Identifier) 관리**: 프로젝트가 삭제되거나 재생성되어도 시설물의 논리적 식별자(`total_id`)가 변하지 않도록 전역 시퀀스 레지스트리 및 아카이브 테이블을 운영합니다.
- **동적 가상 통합 뷰 자동 빌드**: 프로젝트별로 물리 분할된 시설물 테이블들의 컬럼 합집합을 계산하여 `facility_total_view`를 자동으로 재구성(Dynamic View Generation)합니다.
- **현장 음성 메모 STT(Speech-to-Text) 전사**: 음성 메모 파일 수집 시 텍스트 전사 파이프라인을 구동하여 시설물 속성 필드로 자동 적재합니다.

---

## 2. 기술 스택

- **언어 및 런타임**: Python 3.10+
- **공간 데이터 및 ETL**: GeoPandas, GDAL, Fiona, Shapely
- **데이터베이스 드라이버**: `psycopg2` (PostgreSQL 17 / PostGIS 3.4)
- **오디오 처리 & STT**: SpeechRecognition, pydub
- **배포 환경**: Docker 컨테이너 (Kubernetes 단일 워커 Pod)

---

## 3. 동기화 파이프라인 및 업무 프로세스

### 3.1 ETL 데이터 파이프라인 흐름

```
[현장조사 모바일 앱] ──(조사 완료 업로드)──> [QFieldCloud]
                                               │
               ┌─ 30초 주기 감지 및 동기화 ──────┘
               ▼
       [sj-qfieldsync 워커]
         ├── 1. QFieldCloud 메타 DB 조회 (delta_apply 완료 타임스탬프 비교)
         ├── 2. 변경된 프로젝트의 최신 GPKG 패키지 다운로드
         ├── 3. 레이어별 대상 물리 테이블 존재 확인 및 컬럼 자동 보강 (ALTER TABLE)
         ├── 4. Partial Unique Index 기반 멱등 UPSERT (use_yn='y')
         ├── 5. 음성 메모 컬럼 감지 시 STT 비동기 텍스트 추출
         └── 6. 전체 활성 테이블 컬럼 합집합 기반 qfield.facility_total_view 갱신
               │
               ▼
       [PostgreSQL / PostGIS]
         ├─ qfield.table_seq_registry (테이블 식별자 고정)
         ├─ qfield.owner_project_layer (물리 시설물 테이블들)
         ├─ qfield.facility_deleted_archive (삭제된 프로젝트 아카이브)
         └─ qfield.facility_total_view (백엔드 서빙용 통합 뷰)
```

---

## 4. 핵심 엔지니어링 구현 상세

### 4.1 변경분 증분 감지 (Incremental Change Detection)
전수 스캔으로 인한 I/O 병목을 제거하기 위해 QFieldCloud 내부 메타 DB와 연동하여 동기화를 최적화했습니다:
- 프로젝트의 마지막 델타 적용 작업(`delta_apply`) 완료 시각을 내부 메모리 캐시와 대조.
- 실제로 변경이 발생한 프로젝트의 GPKG만 선별 다운로드하여 네트워크 대역폭과 DB 트랜잭션을 최소화.

### 4.2 참조 무결성을 위한 영구 불변 ID 보장 체계
지도 서비스의 후속 업무(내업 기록, 보수 사진)는 시설물의 `total_id`를 논리적 외래키(FK)로 참조합니다. QFieldCloud에서 현장 프로젝트가 삭제되거나 재등록되어도 기존 업무 기록이 어긋나지 않도록 강력한 ID 보존 메커니즘을 구현했습니다:
- `table_seq_registry` 테이블을 통해 프로젝트 테이블별 `table_idx`를 영구 발급.
- 고유 식별자 계산: `total_seq = (table_idx * 10,000,000) + own_id`, `total_id = 'FACIL_T{table_idx}_{own_id}'`.
- 프로젝트 삭제 시 물리 테이블을 즉시 DROP하지 않고, 모든 행을 `facility_deleted_archive`로 옮겨 원래의 `total_seq`/`total_id`를 보존한 후 정리.

### 4.3 소프트 삭제 및 이력 보존 (Partial Unique Index)
동기화 중 데이터 삭제 시 과거 이력을 보존하면서 무결성을 유지하기 위해 조건부 고유 인덱스를 적용했습니다:
- `ON CONFLICT (src_key) WHERE use_yn = 'y'` 인덱스를 선언.
- 현장 소스에서 제거된 데이터는 물리 삭제 없이 `use_yn = 'n'`으로 플래그 처리하여 현재 활성 데이터와 과거 삭제 이력이 안전하게 공존.

### 4.4 동적 통합 뷰 생성 (Dynamic Schema Union)
서로 다른 현장 조사 양식으로 인해 테이블마다 속성 컬럼이 상이합니다.
- 테이블 추가/변경 시 시스템 카탈로그에서 모든 활성 테이블의 컬럼 목록을 추출하여 합집합 생성.
- 특정 테이블에 없는 컬럼은 `NULL AS 컬럼명`으로 보정하고 `UNION ALL`로 연결하는 `CREATE OR REPLACE VIEW qfield.facility_total_view`를 동적 생성하여 상위 백엔드에 단일화된 스키마 인터페이스를 제공.

---

## 5. 실행 및 운영 가이드

### 단독 워커 실행
```bash
# 의존성 설치
pip install -r requirements.txt

# 동기화 데몬 구동
python qfield_data_sync.py
```

### 소스코드 문법 사전 검증
```bash
python -m py_compile qfield_data_sync.py
```

> **주의**: 로컬 환경에서 기동 시 원격 PostGIS DB에 실제로 연결되어 동기화가 실행되므로, 개발/검증 시 연결 정보(`FLASK_ENV`)를 확인하십시오.
