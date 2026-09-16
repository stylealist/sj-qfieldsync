"""
QFieldCloud -> PostGIS 실시간 동기화 엔진
(qfield_info 기반 컬럼 필터링 + own_id/src_key 이원화 UPSERT 풀버전)

기능
1) QFieldCloud의 전체 프로젝트를 주기적으로 스캔
2) 신규/변경된 프로젝트만 SDK로 다운로드
3) qfield.qfield_info 테이블의 column_list와 일치하는 유효 레이어만 PostGIS 물리 테이블로 UPSERT 적재 (+ 음성 STT 변환)
4) QFieldCloud에서 삭제된 프로젝트는 물리 테이블 데이터를 facility_deleted_archive로 이관(use_yn='n') 후 원본 테이블 DROP 처리
   (이관 시점의 total_seq/total_id를 그대로 고정 보존하여 삭제 전/후 값이 절대 바뀌지 않음)
5) own_id를 기반으로 직관적인 고유 Key(FACIL_T{idx}_{own_id})를 제공하는 qfield.facility_total_view 자동 생성/갱신
   (테이블별 idx는 table_seq_registry에 영구 고정되어 테이블 추가/삭제와 무관하게 유지됨)
"""

import os
import re
import time
import shutil
import hashlib
from datetime import datetime

import fiona
import geopandas as gpd
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_batch, execute_values
from shapely.wkb import dumps as wkb_dumps
from sqlalchemy import URL, create_engine, text
from qfieldcloud_sdk import sdk

# STT(음성 -> 텍스트) 모듈
# ImportError 뿐 아니라 의존 라이브러리 초기화 실패도 잡아야 원인을 알 수 있다.
try:
    import disaster2convert as dc
except Exception as _stt_import_error:
    dc = None
    print(f"⚠️ STT 모듈 로드 실패({type(_stt_import_error).__name__}): {_stt_import_error}. "
          f"음성 텍스트 변환이 비활성화됩니다.", flush=True)

# STT 대상 오디오 확장자 (이 확장자를 가진 파일 값일 때만 변환을 시도한다)
AUDIO_EXTENSIONS = (".m4a", ".wav", ".mp3", ".aac", ".ogg", ".amr", ".3gp")


# ============================================================
# 1. 설정 (Configuration)
# ============================================================

# --- QFieldCloud API 접속 정보 ---
QFC_URL = "https://qfield.sj-lab.co.kr/api/v1/"
QFC_USERNAME = "admin"
QFC_PASSWORD = "!@xogml159"

# --- QFieldCloud 메타데이터 DB (프로젝트/유저/작업이력) ---
QFC_DB = dict(
    host="211.188.62.54",
    port=5433,
    dbname="qfieldcloud",
    user="stylealist",
    password="!@xogml159",
)

# --- 최종 데이터가 적재될 PostGIS DB ---
DATA_DB = dict(
    host="211.188.62.54",
    port=30017,
    dbname="sjlab",
    user="stylealist",
    password="!@xogml159",
)

# --- 로컬 다운로드 경로 ---
ENV = os.getenv("FLASK_ENV", "local")
BASE_OUTPUT_DIR = "D:/work/qfield" if ENV == "local" else "/app/webfiles/qfield"
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# --- 데이터 적재 대상 스키마 ---
TARGET_SCHEMA = "qfield"

# --- 감시 주기(초) ---
CHECK_INTERVAL = 30

# --- 시스템 관리용 테이블 (일반 물리 테이블 스캔에서 제외) ---
SYSTEM_TABLES = ("qfield_info", "facility_deleted_archive", "table_seq_registry")


def log(msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ============================================================
# 2. DB / SDK 연결 헬퍼
# ============================================================

def get_data_conn():
    """최종 적재용(PostGIS) psycopg2 커넥션 (타임아웃 적용)"""
    return psycopg2.connect(**DATA_DB, connect_timeout=5, options="-c statement_timeout=30000")


def get_qfc_conn():
    """QFieldCloud 메타데이터용 psycopg2 커넥션 (타임아웃 적용)"""
    return psycopg2.connect(**QFC_DB, connect_timeout=5, options="-c statement_timeout=10000")


def login_client():
    try:
        c = sdk.Client(url=QFC_URL)
        c.login(username=QFC_USERNAME, password=QFC_PASSWORD)
        log("✅ QFieldCloud 로그인 성공")
        return c
    except Exception as e:
        log(f"❌ QFieldCloud 로그인 실패: {e}")
        return None


client = login_client()


def _is_audio_column(column_name: str, gdf) -> bool:
    """
    이 컬럼이 오디오 첨부 컬럼인지 판단한다.
    이름 규칙(record/audio/memo)만으로는 텍스트 입력 필드까지 잡히므로,
    실제 값 중 하나라도 오디오 확장자를 가리키면 오디오 컬럼으로 본다.
    값이 모두 비어 있는 경우에는 이름에 audio/record 가 있을 때만 인정한다.
    """
    lowered = column_name.lower()
    if not ("record" in lowered or "audio" in lowered or "memo" in lowered):
        return False

    try:
        values = gdf[column_name].dropna()
    except Exception:
        values = []

    has_value = False
    for value in values:
        text = str(value).strip().lower()
        if not text:
            continue
        has_value = True
        if text.endswith(AUDIO_EXTENSIONS):
            return True

    # 값이 하나도 없으면 이름으로 판단 (첫 동기화 시 컬럼을 만들어 두기 위함)
    return not has_value and ("audio" in lowered or "record" in lowered)


def build_audio_cache(project_path):
    cache = {}
    for root, _, files in os.walk(project_path):
        for f in files:
            cache[f] = os.path.join(root, f)
    return cache


def get_target_columns(qfield_type: str = "facility") -> list:
    """
    qfield.qfield_info 테이블에서 대상 qfield_type의 column_list 배열을 조회
    """
    conn = None
    target_columns = []
    try:
        conn = get_data_conn()
        cur = conn.cursor()
        cur.execute(
            f"SELECT column_list FROM {TARGET_SCHEMA}.qfield_info WHERE qfield_type = %s LIMIT 1",
            (qfield_type,)
        )
        row = cur.fetchone()
        if row and row[0]:
            target_columns = list(row[0])
    except Exception as e:
        log(f"⚠️ qfield_info 조회 실패 ({qfield_type}): {e}")
    finally:
        if conn:
            conn.close()
    return target_columns


# ============================================================
# 3. 권한 부여 및 데이터 적재
# ============================================================

def grant_admin_permission_via_db(project_id):
    """admin 계정에 프로젝트 admin 권한을 DB로 직접 부여"""
    conn = None
    try:
        conn = get_qfc_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM public.core_user WHERE username = %s", (QFC_USERNAME,))
        row = cur.fetchone()
        if row:
            admin_id = row[0]
            cur.execute(
                """
                INSERT INTO public.core_projectcollaborator
                    (project_id, collaborator_id, role, created_at, updated_at,
                     created_by_id, updated_by_id, is_incognito)
                VALUES (%s, %s, 'admin', NOW(), NOW(), %s, %s, false)
                ON CONFLICT (project_id, collaborator_id) DO NOTHING
                """,
                (project_id, admin_id, admin_id, admin_id),
            )
            conn.commit()
    except Exception as e:
        log(f"⚠️ 권한 부여 중 에러 ({project_id}): {e}")
    finally:
        if conn:
            conn.close()


def _parse_timestamp(val):
    """일시 컬럼 값을 datetime으로 파싱"""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(val, datetime):
        return val

    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        formats = [
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
            "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y%m%d%H%M%S", "%Y%m%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
        log(f"⚠️ 일시 데이터 파싱 실패: '{val}'")
        return None

    return None


def save_gdf_direct(gdf, table_name, schema, project_path, allowed_columns=None):
    """
    GeoDataFrame을 물리 테이블에 UPSERT 적재 (qfield_info 컬럼 기준 필터링 적용)
    """
    log(f"💾 [DB 저장 시작] {table_name}")
    conn = None
    try:
        if "fid" not in gdf.columns:
            raise ValueError("GeoDataFrame에 소스 fid 컬럼이 없습니다.")

        conn = get_data_conn()
        cur = conn.cursor()

        is_geo = isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None
        geom_col = gdf.geometry.name if is_geo else None

        # qfield_info에 정의된 allowed_columns만 필터링 (없으면 기존 방식대로 전체)
        if allowed_columns:
            source_cols = [c for c in allowed_columns if c in gdf.columns]
        else:
            reserved_cols = {"fid", "own_id", "src_key", "seq", "platform_type", "use_yn", "reg_date", "update_at"}
            if geom_col:
                reserved_cols.add(geom_col.lower())
            source_cols = [c for c in gdf.columns if c.lower() not in reserved_cols]

        # 음성 첨부 컬럼에는 STT 결과를 담을 <컬럼>_txt 를 함께 만든다.
        # 이름에 memo 가 들어가도 facility_memo 처럼 텍스트 입력 필드가 있어,
        # 실제 값이 오디오 파일 경로인 컬럼만 대상으로 삼는다(빈 _txt 컬럼 양산 방지).
        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if _is_audio_column(c, gdf):
                final_cols.append(c + "_txt")

        date_cols = {c for c in final_cols if "date" in c.lower() or "time" in c.lower() or "at" in c.lower()}

        # 1. 테이블 생성 및 컬럼 보강
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table_name),
        )
        existing_cols = {r[0] for r in cur.fetchall()}
        table_exists = bool(existing_cols)

        if not table_exists:
            col_defs = ["own_id BIGSERIAL PRIMARY KEY", "src_key TEXT"]
            for col in final_cols:
                if col in date_cols:
                    col_defs.append(f'"{col}" TIMESTAMP')
                else:
                    col_defs.append(f'"{col}" TEXT')

            col_defs.append("use_yn CHAR(1) DEFAULT 'y'")
            col_defs.append("reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            col_defs.append("update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

            if is_geo:
                col_defs.append(f'"{geom_col}" GEOMETRY')

            cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

            active_uq_name = f"ux_{table_name}_srckey_active"
            cur.execute(
                f'CREATE UNIQUE INDEX "{active_uq_name}" '
                f'ON {schema}."{table_name}" (src_key) WHERE use_yn = \'y\''
            )

            if is_geo:
                index_name = f"idx_{table_name}_{geom_col}"
                cur.execute(
                    f'CREATE INDEX "{index_name}" ON {schema}."{table_name}" USING GIST ("{geom_col}")'
                )
                log(f"🗂️ 인덱스 생성: {index_name}")

            conn.commit()
        else:
            for col in final_cols:
                if col not in existing_cols:
                    col_type = "TIMESTAMP" if col in date_cols else "TEXT"
                    cur.execute(
                        f'ALTER TABLE {schema}."{table_name}" ADD COLUMN IF NOT EXISTS "{col}" {col_type}'
                    )
            if is_geo and geom_col not in existing_cols:
                cur.execute(
                    f'ALTER TABLE {schema}."{table_name}" ADD COLUMN IF NOT EXISTS "{geom_col}" GEOMETRY'
                )
            conn.commit()

        audio_cache = build_audio_cache(project_path)

        # 2. UPSERT 쿼리 생성
        insert_cols = ["src_key"] + final_cols + ["use_yn"]
        if is_geo:
            insert_cols.append(geom_col)

        placeholders = []
        for col in insert_cols:
            if col in date_cols:
                placeholders.append("%s::timestamp")
            elif col == geom_col:
                placeholders.append("%s::geometry")
            else:
                placeholders.append("%s")

        quoted_insert_cols = ", ".join(f'"{c}"' for c in insert_cols)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in insert_cols if c != "src_key"
        )
        update_set += ', "update_at" = NOW()'

        sql = (
            f'INSERT INTO {schema}."{table_name}" '
            f'({quoted_insert_cols}) '
            f'VALUES ({", ".join(placeholders)}) '
            f"ON CONFLICT (src_key) WHERE use_yn = 'y' DO UPDATE SET {update_set}"
        )

        # 3. 데이터 배치 바인딩
        batch_data = []
        current_keys = []
        for row in gdf.itertuples(index=False):
            row_dict = row._asdict()
            src_key_val = str(row_dict.get("fid"))
            current_keys.append(src_key_val)
            values = [src_key_val]

            for col in final_cols:
                if col.endswith("_txt"):
                    origin = col[:-4]
                    file = row_dict.get(origin)
                    stt_val = ""

                    if isinstance(file, str) and file.strip():
                        if not dc:
                            log(f"⚠️ STT 건너뜀(모듈 없음): {table_name}.{origin}={file}")
                        else:
                            path = audio_cache.get(os.path.basename(file))
                            if not path:
                                # 다운로드된 프로젝트 폴더에 파일이 없을 때 (부분 다운로드 등)
                                log(f"⚠️ STT 건너뜀(파일 없음): {table_name}.{origin}={file}")
                            else:
                                try:
                                    stt_val = dc.read_audio(path)
                                    if stt_val:
                                        log(f"🗣️ STT 성공: {os.path.basename(path)} → {len(stt_val)}자")
                                    else:
                                        # 인식 결과가 비어 있는 경우(무음·인식 실패·API 오류)
                                        log(f"⚠️ STT 결과 없음: {os.path.basename(path)}")
                                except Exception as e:
                                    # 조용히 삼키면 원인을 추적할 수 없다
                                    log(f"❌ STT 실패({type(e).__name__}): {os.path.basename(path)} - {e}")

                    values.append(stt_val)
                elif col in date_cols:
                    values.append(_parse_timestamp(row_dict.get(col)))
                else:
                    val = row_dict.get(col)
                    values.append(None if pd.isna(val) else str(val))

            values.append("y")  # use_yn
            if is_geo:
                geom = row_dict.get(geom_col)
                values.append(wkb_dumps(geom, output_dimension=2, hex=True, srid=3857) if geom else None)
            batch_data.append(values)

        execute_batch(cur, sql, batch_data, page_size=1000)

        # 4. 소스에서 삭제된 활성 src_key 소프트 삭제
        if current_keys:
            cur.execute(
                f'UPDATE {schema}."{table_name}" '
                f"SET use_yn = 'n', update_at = NOW() "
                f"WHERE use_yn = 'y' AND src_key <> ALL(%s)",
                (current_keys,),
            )

        conn.commit()
        log(f"✅ [DB 저장 성공] {table_name} ({len(batch_data)}건 upsert)")

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"❌ [DB 저장 실패] {table_name}: {e}")
    finally:
        if conn:
            conn.close()


# ============================================================
# 4. facility_total_view 통합 뷰 생성 함수
# ============================================================

def update_facility_total_view():
    """
    TARGET_SCHEMA 내의 모든 물리 테이블을 스캔하여
    own_id 기반 안정적인 고유 Key(FACIL_T{idx}_{own_id})가 포함된 facility_total_view 생성/갱신
    + 삭제된 프로젝트의 아카이브 데이터(facility_deleted_archive)도 use_yn='n'으로 포함

    ⚠️ 활성 테이블의 idx는 table_seq_registry에 영구 고정되어 테이블 추가/삭제와 무관하게 유지되고,
       archive로 이관된 데이터는 이관 시점에 저장해둔 orig_total_seq/orig_total_id를 그대로 사용하므로
       "삭제 전/후로 total_seq, total_id가 절대 바뀌지 않는다."
    """
    log("📊 [facility_total_view 갱신 시작]")
    conn = None
    try:
        conn = get_data_conn()
        conn.autocommit = True
        cur = conn.cursor()

        # 1. qfield 스키마 내 물리 테이블 목록 조회 (시스템 테이블 제외)
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
              AND table_name NOT IN %s
            ORDER BY table_name
        """, (TARGET_SCHEMA, SYSTEM_TABLES))
        tables = [r[0] for r in cur.fetchall()]

        # 아카이브 테이블 존재 여부 확인 (없으면 아직 프로젝트 삭제 이력이 없는 것)
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = 'facility_deleted_archive'
        """, (TARGET_SCHEMA,))
        archive_exists = bool(cur.fetchone())

        if not tables and not archive_exists:
            log("⚠️ [뷰 생성 스킵] 스키마 내에 물리 테이블이 존재하지 않습니다.")
            cur.execute(f"DROP VIEW IF EXISTS {TARGET_SCHEMA}.facility_total_view CASCADE")
            return

        # 2. 모든 테이블의 컬럼 집합 수집
        table_col_map = {}
        all_unique_cols = set()
        system_cols = {"own_id", "src_key", "fid", "seq", "project_id", "project_name", "owner", "use_yn", "reg_date", "update_at", "geometry", "geom"}

        valid_tables = []
        for t_name in tables:
            cur.execute("""
                SELECT column_name, udt_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (TARGET_SCHEMA, t_name))
            cols = {r[0].lower(): (r[0], r[1]) for r in cur.fetchall()}

            if "project_id" in cols or "geometry" in cols or "geom" in cols or "own_id" in cols:
                table_col_map[t_name] = cols
                valid_tables.append(t_name)
                for c_lower, (c_orig, _) in cols.items():
                    if c_lower not in system_cols:
                        all_unique_cols.add(c_orig)

        if not valid_tables and not archive_exists:
            log("⚠️ [뷰 생성 스킵] 동기화 대상 물리 테이블이 없습니다.")
            return

        # 3. 정렬된 통합 컬럼 목록 정의
        sorted_custom_cols = sorted(list(all_unique_cols))

        # 4. 각 물리 테이블별 SELECT 쿼리문 조합
        # idx는 매번 스캔 순서가 아니라 table_seq_registry에 영구 고정된 값을 사용
        # (테이블이 삭제/추가되어도 기존 테이블들의 total_seq/total_id가 절대 바뀌지 않음)
        table_idx_map = ensure_table_idx_registry(cur, valid_tables)

        union_parts = []
        for t_name in valid_tables:
            idx = table_idx_map[t_name]
            t_cols = table_col_map[t_name]
            table_prefix_num = idx * 10000000

            if "own_id" in t_cols:
                own_id_col = t_cols["own_id"][0]
                origin_id_val = f't."{own_id_col}"::BIGINT'
                total_seq_expr = f'({table_prefix_num} + t."{own_id_col}")::BIGINT'
                total_id_expr = f"'FACIL_T{idx}_' || t.\"{own_id_col}\"::TEXT"
            else:
                origin_id_val = "NULL::BIGINT"
                total_seq_expr = f"({table_prefix_num})::BIGINT"
                total_id_expr = f"'FACIL_T{idx}_0'"

            src_key_val = f't."{t_cols["src_key"][0]}"' if "src_key" in t_cols else "NULL::TEXT"

            # geom 컬럼명 탐색
            geom_val = "NULL::GEOMETRY"
            if "geometry" in t_cols:
                geom_val = f't."{t_cols["geometry"][0]}"'
            elif "geom" in t_cols:
                geom_val = f't."{t_cols["geom"][0]}"'

            # 기본 메타데이터 컬럼 매핑
            proj_id_val = f't."{t_cols["project_id"][0]}"' if "project_id" in t_cols else "NULL::TEXT"
            proj_name_val = f't."{t_cols["project_name"][0]}"' if "project_name" in t_cols else "NULL::TEXT"
            owner_val = f't."{t_cols["owner"][0]}"' if "owner" in t_cols else "NULL::TEXT"
            use_yn_val = f't."{t_cols["use_yn"][0]}"' if "use_yn" in t_cols else "'y'::CHAR(1)"
            reg_date_val = f't."{t_cols["reg_date"][0]}"' if "reg_date" in t_cols else "NULL::TIMESTAMP"
            update_at_val = f't."{t_cols["update_at"][0]}"' if "update_at" in t_cols else "NULL::TIMESTAMP"

            # 사용자 정의 속성 매핑 (없으면 NULL)
            custom_selects = []
            for col in sorted_custom_cols:
                col_lower = col.lower()
                if col_lower in t_cols:
                    orig_name, udt = t_cols[col_lower]
                    if udt in ("timestamp", "timestamptz"):
                        custom_selects.append(f't."{orig_name}"::TIMESTAMP AS "{col}"')
                    else:
                        custom_selects.append(f't."{orig_name}"::TEXT AS "{col}"')
                else:
                    custom_selects.append(f'NULL::TEXT AS "{col}"')

            custom_selects_str = ",\n        ".join(custom_selects)

            part = f"""
    SELECT
        {total_seq_expr} AS total_seq,
        {total_id_expr} AS total_id,
        {origin_id_val} AS origin_id,
        {src_key_val} AS source_fid,
        '{t_name}'::TEXT AS source_table,
        {proj_id_val} AS project_id,
        {proj_name_val} AS project_name,
        {owner_val} AS owner,
        {custom_selects_str},
        {use_yn_val} AS use_yn,
        {reg_date_val} AS reg_date,
        {update_at_val} AS update_at,
        {geom_val} AS geom
    FROM {TARGET_SCHEMA}."{t_name}" t
    """
            union_parts.append(part)

        # 5. 삭제된 프로젝트의 아카이브 데이터도 use_yn='n'으로 뷰에 포함
        # 삭제 전에 저장해둔 orig_total_seq/orig_total_id를 그대로 사용 -> 삭제 전/후 값이 절대 바뀌지 않음
        # (혹시 orig_total_seq가 없는 예전 레코드는 archive_prefix 기반 값으로 폴백)
        if archive_exists:
            archive_prefix = 10 ** 15  # 폴백용 여유 구간

            archive_custom_selects = []
            for col in sorted_custom_cols:
                col_lower = col.lower()
                if col_lower.endswith("date") or col_lower.endswith("time") or col_lower.endswith("at"):
                    archive_custom_selects.append(
                        f"NULLIF(a.attrs->>'{col}', '')::TIMESTAMP AS \"{col}\""
                    )
                else:
                    archive_custom_selects.append(f"a.attrs->>'{col}' AS \"{col}\"")
            archive_custom_str = ",\n        ".join(archive_custom_selects)
            archive_custom_line = f"{archive_custom_str}," if archive_custom_str else ""

            archive_part = f"""
    SELECT
        COALESCE(a.orig_total_seq, ({archive_prefix} + a.id))::BIGINT AS total_seq,
        COALESCE(a.orig_total_id, 'FACIL_ARCH_' || a.id::TEXT) AS total_id,
        a.own_id AS origin_id,
        a.src_key AS source_fid,
        a.source_table AS source_table,
        a.project_id AS project_id,
        a.project_name AS project_name,
        a.owner AS owner,
        {archive_custom_line}
        a.use_yn AS use_yn,
        a.reg_date AS reg_date,
        a.update_at AS update_at,
        a.geom AS geom
    FROM {TARGET_SCHEMA}.facility_deleted_archive a
    """
            union_parts.append(archive_part)

        if not union_parts:
            log("⚠️ [뷰 생성 스킵] UNION 대상이 없습니다.")
            cur.execute(f"DROP VIEW IF EXISTS {TARGET_SCHEMA}.facility_total_view CASCADE")
            return

        # 6. facility_total_view 생성 SQL 실행
        union_all_sql = "\n    UNION ALL\n".join(union_parts)
        create_view_sql = f"""
CREATE OR REPLACE VIEW {TARGET_SCHEMA}.facility_total_view AS
{union_all_sql};
"""
        cur.execute(f"DROP VIEW IF EXISTS {TARGET_SCHEMA}.facility_total_view CASCADE")
        cur.execute(create_view_sql)
        log(f"✅ [facility_total_view 갱신 완료] 물리 테이블 {len(valid_tables)}개"
            f" + 아카이브 {'포함' if archive_exists else '미포함'}"
            f" 통합됨 (total_id: FACIL_T{{idx}}_{{own_id}} / FACIL_ARCH_{{id}}, idx는 영구 고정)")

    except Exception as e:
        log(f"❌ [facility_total_view 갱신 실패]: {e}")
    finally:
        if conn:
            conn.close()


# ============================================================
# 5. GPKG 분석 및 적재
# ============================================================

def _read_gpkg_layer_with_fid(gpkg_path, layer_name):
    """
    GPKG 레이어를 읽으면서 원본 feature id(fid)를 'fid' 컬럼으로 보존
    """
    try:
        gdf = gpd.read_file(gpkg_path, layer=layer_name, fid_as_index=True)
        gdf = gdf.reset_index()
        if "index" in gdf.columns and "fid" not in gdf.columns:
            gdf = gdf.rename(columns={"index": "fid"})
    except TypeError:
        gdf = gpd.read_file(gpkg_path, layer=layer_name)
        if "fid" not in gdf.columns:
            raise ValueError(
                f"'{layer_name}' 레이어에서 fid를 확보할 수 없습니다."
            )
    return gdf


def _slugify_table_part(text_val: str, maxlen: int = 40) -> str:
    """테이블명 조각을 안전하고 고정된 형태로 변환"""
    slug = re.sub(r"[^0-9a-zA-Z_]+", "_", text_val.strip().lower()).strip("_")
    if not slug:
        slug = "layer"
    if len(slug) > maxlen:
        h = hashlib.md5(text_val.encode("utf-8")).hexdigest()[:8]
        slug = f"{slug[:maxlen - 9]}_{h}"
    return slug


def soft_delete_all_active(table_name, schema):
    """레이어 전체 삭제 시 활성 행 소프트 삭제"""
    conn = None
    try:
        conn = get_data_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table_name),
        )
        if not cur.fetchone():
            return

        cur.execute(
            f'UPDATE {schema}."{table_name}" SET use_yn = \'n\', update_at = NOW() WHERE use_yn = \'y\''
        )
        if cur.rowcount:
            log(f"🧹 [레이어 전체 삭제 감지] {table_name}: {cur.rowcount}건 소프트 삭제")
    except Exception as e:
        log(f"⚠️ [빈 레이어 소프트 삭제 오류] {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """
    프로젝트 폴더 내 GPKG 레이어 중 qfield_info의 column_list에 매칭되는 레이어만 적재
    """
    log(f"🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    clean_owner = owner.lower().replace(" ", "_").replace("-", "_")

    if not os.path.exists(project_path):
        return False

    gpkg_files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]
    if not gpkg_files:
        return False

    # 💡 qfield.qfield_info에서 facility의 column_list 조회
    target_columns = get_target_columns("facility")
    target_col_set = {c.lower() for c in target_columns}

    any_saved = False

    for file in gpkg_files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]

        try:
            layers = fiona.listlayers(gpkg_path)
        except Exception as e:
            log(f"⚠️ {file} 레이어 목록 조회 실패: {e}")
            continue

        for layer_name in layers:
            if layer_name.lower() in {"layer_styles", "geopackage_contents", "gpkg_contents"}:
                continue

            layer_key = _slugify_table_part(f"{file_stem}_{layer_name}")
            table_name = f"{clean_owner}_{short_id}_{layer_key}"

            try:
                gdf = _read_gpkg_layer_with_fid(gpkg_path, layer_name)

                # 💡 [핵심 필터링]: qfield_info의 column_list와 레이어 컬럼 비교
                if target_col_set:
                    layer_cols_set = {c.lower() for c in gdf.columns}
                    # target_column 중 레이어에 존재하는 컬럼 개수 계산
                    intersection = target_col_set.intersection(layer_cols_set)
                    
                    # 기준 컬럼이 1개도 일치하지 않는 무관한 레이어는 스킵
                    if not intersection:
                        log(f"⏭️ [스킵] '{layer_name}' 레이어는 qfield_info 컬럼 정의와 일치하지 않음")
                        continue

                if gdf.empty:
                    soft_delete_all_active(table_name, TARGET_SCHEMA)
                    continue

                # 좌표계 3857 변환
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=4326).to_crs(epsg=3857)

                # 메타데이터 주입
                gdf = gdf.assign(
                    project_id=project_id,
                    project_name=project_name,
                    owner=owner
                )

                # qfield_info에 정의된 컬럼 위주로 저장
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, allowed_columns=target_columns)
                any_saved = True

            except Exception as e:
                log(f"⚠️ '{layer_name}' 처리 중 에러: {e}")

    return any_saved


# ============================================================
# 6. 프로젝트 동기화 / 삭제 감지
# ============================================================

def sync_single_project(project_data):
    global client
    p_id, p_name, p_owner = project_data["id"], project_data["name"], project_data["owner"]
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

    log(f"🔑 [1/4 권한 확인/부여] {p_name}")
    grant_admin_permission_via_db(p_id)

    log(f"📁 [2/4 로컬 디렉토리 준비] {p_name}")
    try:
        if os.path.exists(project_path):
            shutil.rmtree(project_path, ignore_errors=True)
        os.makedirs(project_path, exist_ok=True)
    except Exception as e:
        log(f"⚠️ 디렉토리 정리 오류: {e}")

    matched = False
    try:
        log(f"🚀 [3/4 SDK 다운로드 시작] {p_name}")
        if not client:
            client = login_client()

        client.download_project(
            project_id=p_id,
            local_dir=project_path,
            filter_glob="*",
            show_progress=False,
            force_download=True,
        )
        log(f"✅ [3/4 SDK 다운로드 완료] {p_name}")

        log(f"🔍 [4/4 PostGIS 적재 시작] {p_name}")
        matched = process_gpkg_to_db(p_id, project_path, p_name, p_owner)

        if not matched:
            log(f"🗑️ [파일 삭제] '{p_name}' 적재된 유효 레이어 없음")
            shutil.rmtree(project_path, ignore_errors=True)

    except Exception as e:
        if "401" in str(e) or "Unauthorized" in str(e):
            client = login_client()
        log(f"⚠️ {p_name} 처리 실패: {e}")

    return matched


def get_latest_job_id(project_id):
    """delta_apply 완료 시각 기준으로 변경 여부 판단"""
    conn = None
    try:
        conn = get_qfc_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT id, finished_at FROM public.core_job
            WHERE project_id = %s::uuid AND type = 'delta_apply' AND status = 'finished'
            ORDER BY finished_at DESC LIMIT 1
        """, (project_id,))
        job_row = cur.fetchone()
        if job_row and job_row["finished_at"]:
            return f"delta_{job_row['id']}_{job_row['finished_at']}"

        cur.execute("SELECT id, data_last_updated_at FROM public.core_project WHERE id = %s::uuid", (project_id,))
        proj_row = cur.fetchone()
        if not proj_row:
            return "PROJECT_NOT_FOUND"
        if not proj_row["data_last_updated_at"]:
            return "NO_JOB"
        return f"proj_{proj_row['data_last_updated_at']}"

    except Exception as e:
        log(f"⚠️ [Job DB 조회 오류] {project_id}: {e}")
        return "JOB_CHECK_ERROR"
    finally:
        if conn:
            conn.close()


def get_all_projects_from_db():
    """QFieldCloud 전체 프로젝트 목록 조회"""
    projects = []
    conn = None
    try:
        conn = get_qfc_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT p.id, p.name, u.username AS owner_name
            FROM public.core_project p
            JOIN public.core_user u ON p.owner_id = u.id
        """)
        for r in cur.fetchall():
            projects.append({"id": str(r["id"]), "name": r["name"], "owner": r["owner_name"]})
    except Exception as e:
        log(f"⚠️ 운영 DB 조회 에러: {e}")
    finally:
        if conn:
            conn.close()
    return projects


def cleanup_deleted_projects(current_project_ids):
    """
    QFieldCloud에 실제 등록된 프로젝트 목록과 물리 테이블명(short_id)을 직접 비교하여
    현재 클라우드에 존재하지 않는(=삭제된) 프로젝트에 속한 테이블만 정리한다.
    (테이블 내부 데이터가 아니라 테이블명 자체의 short_id로 판단하므로,
     행이 0건이거나 이미 전부 use_yn='n'인 경우에도 누락 없이 감지된다)
    1) 대상 테이블 데이터를 facility_deleted_archive로 이관(use_yn='n') 후 원본 테이블 DROP
    2) 로컬 디렉토리 캐시 삭제
    """
    if not current_project_ids:
        return False

    # 테이블명에 박혀있는 short_id(project_id[:13], 하이픈 포함)와 비교하기 위한 집합
    current_short_ids = {pid[:13] for pid in current_project_ids}

    conn = None
    deleted_any = False
    try:
        conn = get_data_conn()
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
              AND table_name NOT IN %s
        """, (TARGET_SCHEMA, SYSTEM_TABLES))
        tables = [r[0] for r in cur.fetchall()]

        for t_name in tables:
            # 테이블명 안에 현재 클라우드에 존재하는 프로젝트의 short_id가 포함되어 있으면 유지
            if any(short_id in t_name for short_id in current_short_ids):
                continue

            # 현재 클라우드의 어떤 프로젝트와도 매칭되지 않음 -> 삭제된 프로젝트의 잔여 테이블로 판단
            log(f"🔎 [삭제 프로젝트 테이블 감지] {t_name} (클라우드 등록 프로젝트와 매칭되는 short_id 없음)")
            archive_and_drop_table(t_name, TARGET_SCHEMA)
            deleted_any = True

        if os.path.exists(BASE_OUTPUT_DIR):
            local_dirs = [d for d in os.listdir(BASE_OUTPUT_DIR) if os.path.isdir(os.path.join(BASE_OUTPUT_DIR, d))]
            for local_pid in local_dirs:
                if local_pid not in current_project_ids:
                    target_dir = os.path.join(BASE_OUTPUT_DIR, local_pid)
                    shutil.rmtree(target_dir, ignore_errors=True)
                    log(f"🗑️ [로컬 캐시 삭제] {local_pid}")

    except Exception as e:
        log(f"⚠️ 프로젝트 삭제 정리 오류: {e}")
    finally:
        if conn:
            conn.close()

    return deleted_any


def ensure_table_idx_registry(cur, table_names):
    """
    각 물리 테이블에 영구적이고 고유한 idx를 부여/조회한다.
    - table_seq_registry에 한 번 등록된 idx는 테이블이 DROP되어도 절대 재사용되지 않는다.
    - 이를 통해 total_seq/total_id가 테이블 추가/삭제와 무관하게 항상 안정적으로 유지된다.
    """
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.table_seq_registry (
            table_name TEXT PRIMARY KEY,
            table_idx BIGSERIAL UNIQUE,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    table_names = list(table_names)
    if table_names:
        execute_values(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.table_seq_registry (table_name)
            VALUES %s
            ON CONFLICT (table_name) DO NOTHING
            """,
            [(t,) for t in table_names],
        )

    cur.execute(f"""
        SELECT table_name, table_idx FROM {TARGET_SCHEMA}.table_seq_registry
        WHERE table_name = ANY(%s)
    """, (table_names,))
    return {r[0]: r[1] for r in cur.fetchall()}


def archive_and_drop_table(table_name, schema):
    """
    물리 테이블의 모든 데이터를 facility_deleted_archive로 이관(use_yn='n')한 뒤
    원본 물리 테이블 자체는 DROP 하여 정리한다.
    (레이어마다 컬럼 구조가 달라서 커스텀 속성은 JSONB(attrs)로 통합 저장)

    ⚠️ 삭제 전/후로 total_seq, total_id가 절대 바뀌지 않도록,
       이관 시점에 table_seq_registry의 idx + own_id로 "원래 값"을 그대로 계산해서
       orig_total_seq / orig_total_id 컬럼에 고정 저장한다.
       (뷰는 이 값을 그대로 사용하므로 삭제 후에도 total_seq/total_id가 유지됨)
    """
    conn = None
    try:
        conn = get_data_conn()
        cur = conn.cursor()

        # 0. 아카이브 테이블 보장 (+ 원본 total_seq/total_id 보존 컬럼)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.facility_deleted_archive (
                id BIGSERIAL PRIMARY KEY,
                own_id BIGINT,
                src_key TEXT,
                source_table TEXT,
                project_id TEXT,
                project_name TEXT,
                owner TEXT,
                attrs JSONB,
                use_yn CHAR(1) DEFAULT 'n',
                reg_date TIMESTAMP,
                update_at TIMESTAMP,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                geom GEOMETRY,
                orig_total_seq BIGINT,
                orig_total_id TEXT
            )
        """)
        # 기존에 이미 생성돼있던 archive 테이블에도 신규 컬럼 보강
        cur.execute(f"""
            ALTER TABLE {schema}.facility_deleted_archive
            ADD COLUMN IF NOT EXISTS orig_total_seq BIGINT
        """)
        cur.execute(f"""
            ALTER TABLE {schema}.facility_deleted_archive
            ADD COLUMN IF NOT EXISTS orig_total_id TEXT
        """)
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_facility_deleted_archive_srctable
            ON {schema}.facility_deleted_archive (source_table)
        """)
        conn.commit()

        # 1. 대상 테이블 컬럼 구조 조회
        cur.execute("""
            SELECT column_name, udt_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table_name))
        col_rows = cur.fetchall()
        if not col_rows:
            return  # 이미 없는 테이블

        cols_lower_map = {r[0].lower(): r[0] for r in col_rows}
        udt_map = {r[0]: r[1] for r in col_rows}

        geom_col = next((c for c, u in udt_map.items() if u == "geometry"), None)
        project_id_col = cols_lower_map.get("project_id")
        project_name_col = cols_lower_map.get("project_name")
        owner_col = cols_lower_map.get("owner")

        system_cols = {"own_id", "src_key", "use_yn", "reg_date", "update_at"}
        if geom_col:
            system_cols.add(geom_col.lower())
        exclude_orig = {project_id_col, project_name_col, owner_col}

        attrs_cols = [
            orig for lower, orig in cols_lower_map.items()
            if lower not in system_cols and orig not in exclude_orig
        ]

        own_id_expr = 't."own_id"' if "own_id" in cols_lower_map else "NULL"
        src_key_expr = 't."src_key"' if "src_key" in cols_lower_map else "NULL"
        reg_date_expr = 't."reg_date"' if "reg_date" in cols_lower_map else "NULL"
        update_at_expr = 't."update_at"' if "update_at" in cols_lower_map else "NULL"
        geom_expr = f't."{geom_col}"' if geom_col else "NULL"
        project_id_expr = f't."{project_id_col}"' if project_id_col else "NULL"
        project_name_expr = f't."{project_name_col}"' if project_name_col else "NULL"
        owner_expr = f't."{owner_col}"' if owner_col else "NULL"

        json_build = ", ".join(f"'{c}', t.\"{c}\"" for c in attrs_cols)
        attrs_expr = f"jsonb_build_object({json_build})" if attrs_cols else "'{}'::jsonb"

        # 1-1. 이관 직전 시점의 total_seq/total_id를 그대로 재현하기 위해
        #      이 테이블의 registry idx를 조회(없으면 새로 발급)
        table_idx_map = ensure_table_idx_registry(cur, [table_name])
        idx = table_idx_map.get(table_name)
        own_id_col = cols_lower_map.get("own_id")

        if idx is not None and own_id_col:
            orig_total_seq_expr = f'({idx * 10000000} + t."{own_id_col}")::BIGINT'
            orig_total_id_expr = f"'FACIL_T{idx}_' || t.\"{own_id_col}\"::TEXT"
        else:
            orig_total_seq_expr = "NULL::BIGINT"
            orig_total_id_expr = "NULL::TEXT"

        # 2. 데이터 이관 (INSERT ... SELECT)
        insert_sql = f"""
            INSERT INTO {schema}.facility_deleted_archive
                (own_id, src_key, source_table, project_id, project_name, owner,
                 attrs, use_yn, reg_date, update_at, geom, orig_total_seq, orig_total_id)
            SELECT
                {own_id_expr}, {src_key_expr}, %s, {project_id_expr}, {project_name_expr}, {owner_expr},
                {attrs_expr}, 'n', {reg_date_expr}, {update_at_expr}, {geom_expr},
                {orig_total_seq_expr}, {orig_total_id_expr}
            FROM {schema}."{table_name}" t
        """
        cur.execute(insert_sql, (table_name,))
        archived_count = cur.rowcount
        conn.commit()

        # 3. 원본 물리 테이블 DROP
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        conn.commit()
        log(f"🗄️ [아카이브+DROP 완료] {table_name}: {archived_count}건 이관 후 테이블 삭제 (total_seq/total_id 유지)")

    except Exception as e:
        if conn:
            conn.rollback()
        log(f"⚠️ [아카이브/DROP 실패] {table_name}: {e}")
    finally:
        if conn:
            conn.close()


# ============================================================
# 7. 메인 루프
# ============================================================

def main():
    last_jobs_cache = {}
    log("🚀 실시간 동기화 엔진 가동 중...")

    while True:
        try:
            current_projects = get_all_projects_from_db()
            current_project_ids = [p["id"] for p in current_projects]
            
            # 삭제된 프로젝트 정리
            deleted_occurred = cleanup_deleted_projects(current_project_ids)

            cycle_updated = False

            for p in current_projects:
                p_id = p["id"]
                project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
                current_job_id = get_latest_job_id(p_id)

                if current_job_id in ("PROJECT_NOT_FOUND", "JOB_CHECK_ERROR"):
                    continue

                needs_sync = (
                    p_id not in last_jobs_cache
                    or not os.path.exists(project_path)
                    or current_job_id != last_jobs_cache[p_id]
                )

                if needs_sync:
                    log(f"🔄 변경 감지: {p['name']} (소유자: {p['owner']})")
                    saved = sync_single_project(p)
                    last_jobs_cache[p_id] = current_job_id
                    if saved:
                        cycle_updated = True

            # 물리 테이블 생성/수정 또는 삭제가 일어났을 때 뷰 자동 갱신
            if cycle_updated or deleted_occurred:
                update_facility_total_view()

        except Exception as e:
            log(f"⚠️ 루프 에러: {e}")
            global client
            client = login_client()

        log(f"💤 대기중... ({CHECK_INTERVAL}초)")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()