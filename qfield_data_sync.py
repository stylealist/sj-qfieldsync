"""
QFieldCloud -> PostGIS 실시간 동기화 엔진
(own_id/src_key 이원화 UPSERT + 재사용 없는 안정적 total_id 생성 풀버전)

⚠️ 왜 own_id/src_key를 분리했는가
   GPKG의 fid(feature id)는 SQLite rowid 특성이나 QFieldCloud의 패키징(export)
   방식에 따라 "행이 삭제되면 뒤 행의 fid가 앞으로 당겨지는" 현상이 발생할 수 있다.
   즉 소스의 fid는 절대 재사용/재배치되지 않는다는 보장이 없다.
   그래서 우리 DB의 join-key(total_id)는 fid를 직접 쓰지 않고, own_id(BIGSERIAL)라는
   "우리가 발급하고 절대 재사용하지 않는 내부 키"를 기준으로 만든다.

기능
1) QFieldCloud의 전체 프로젝트를 주기적으로 스캔
2) 신규/변경된 프로젝트만 SDK로 다운로드
3) 다운로드된 GPKG 레이어들을 PostGIS 물리 테이블로 UPSERT 적재 (+ 음성 STT 변환)
   - own_id(BIGSERIAL, 절대 재사용 안 됨)를 진짜 PRIMARY KEY로 사용.
   - src_key(소스 fid)는 "현재 활성(use_yn='y') 행 안에서만" unique하도록
     partial unique index로 매칭에 사용.
   - 소스에서 어떤 fid가 사라지면 해당 행은 물리 삭제하지 않고 use_yn='n'만 변경.
     이후 소스에서 같은 fid 값이 재사용되어 나타나도, 그 값은 이미 비활성 상태라
     기존 행과 매칭되지 않고 새로운 own_id(=max+1)로 신규 삽입된다.
     -> "2번 삭제 시 3번의 키가 2번으로 바뀌는" 문제가 발생하지 않는다.
4) QFieldCloud에서 삭제된 프로젝트는 물리 테이블 소프트 삭제(use_yn='n') 처리
5) own_id를 기반으로 직관적인 고유 Key(FACIL_T{idx}_{own_id})를 제공하는
   qfield.facility_total_view 자동 생성/갱신
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
from psycopg2.extras import execute_batch
from shapely.wkb import dumps as wkb_dumps
from sqlalchemy import URL, create_engine, text
from qfieldcloud_sdk import sdk

# STT(음성 -> 텍스트) 모듈
try:
    import disaster2convert as dc
except ImportError:
    dc = None
    print("⚠️ disaster2convert 모듈을 찾을 수 없습니다. STT 기능이 비활성화됩니다.", flush=True)


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


def build_audio_cache(project_path):
    cache = {}
    for root, _, files in os.walk(project_path):
        for f in files:
            cache[f] = os.path.join(root, f)
    return cache


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


def save_gdf_direct(gdf, table_name, schema, project_path):
    """
    GeoDataFrame을 물리 테이블에 UPSERT 적재.

    ⚠️ 핵심 설계: PK를 이원화한다.
      - own_id (BIGSERIAL): 우리 DB가 발급하는 진짜 PK. 절대 재사용되지 않는다.
        total_id/total_seq는 이 값을 기준으로 만든다.
      - src_key (TEXT): 소스(GPKG)의 fid. "현재 활성 행(use_yn='y') 안에서만"
        유일하도록 partial unique index를 걸어 매칭(UPSERT)에만 사용한다.

    동작:
      - 활성 행 중 같은 src_key가 있으면 -> UPDATE (own_id 유지, 내용만 갱신)
      - 활성 행 중 같은 src_key가 없으면 -> INSERT (own_id는 자동으로 max+1)
        · 소스가 fid를 재사용/재배치해도, 이미 비활성화된 src_key는 매칭 대상이
          아니므로 예전 행을 덮어쓰지 않고 항상 새 own_id로 들어간다.
      - 이번 사이클에 없는 기존 활성 src_key -> use_yn='n' 소프트 삭제
        (물리 삭제 안 함 -> own_id/total_id는 영구 보존, 웹사이트 join 안전)
    """
    log(f"💾 [DB 저장 시작] {table_name}")
    conn = None
    try:
        if "fid" not in gdf.columns:
            raise ValueError(
                "GeoDataFrame에 소스 fid 컬럼이 없습니다. "
                "gpd.read_file(..., fid_as_index=True) 로 읽었는지 확인하세요."
            )

        conn = get_data_conn()
        cur = conn.cursor()

        is_geo = isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None
        geom_col = gdf.geometry.name if is_geo else None

        reserved_cols = {"fid", "own_id", "src_key", "seq", "platform_type", "use_yn", "reg_date", "update_at"}
        if geom_col:
            reserved_cols.add(geom_col.lower())

        source_cols = [c for c in gdf.columns if c.lower() not in reserved_cols]

        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if "record" in c.lower() or "audio" in c.lower():
                final_cols.append(c + "_txt")

        date_cols = {c for c in final_cols if "date" in c.lower() or "time" in c.lower() or "at" in c.lower()}

        # 1. 테이블 존재 여부 확인 (더 이상 DROP 하지 않음)
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

            # 활성 행 안에서만 src_key가 유일하도록 partial unique index 생성.
            # -> 소프트 삭제(use_yn='n')된 src_key는 더 이상 매칭 대상이 아니게 되어
            #    이후 재사용되어도 새로운 own_id로 들어간다.
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
            # 스키마가 진화한 경우(신규 필드 추가 등) 컬럼만 보강한다.
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

        # 2. UPSERT 컬럼 및 Placeholders 구성 (src_key 기준, 활성 행에만 매칭)
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

        # ON CONFLICT ... WHERE use_yn='y' 는 위에서 만든 partial unique index를
        # arbiter로 지정하는 것과 동일 -> 비활성 행과는 절대 충돌(=덮어쓰기)하지 않는다.
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
                    if isinstance(file, str) and file.strip() and dc:
                        path = audio_cache.get(os.path.basename(file))
                        if path:
                            try:
                                stt_val = dc.read_audio(path)
                            except Exception:
                                pass
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

        # 4. 이번 사이클에 없는 기존 활성 src_key는 삭제된 피처로 간주하여 소프트 삭제.
        #    own_id/행 자체는 물리 삭제하지 않으므로 total_id 참조 무결성이 깨지지 않고,
        #    이 src_key가 나중에 재사용되어도 위 partial unique index 덕분에
        #    이 행과는 다시 매칭되지 않는다(항상 새 own_id로 신규 삽입됨).
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
    own_id(우리 DB가 발급한, 절대 재사용되지 않는 내부 PK)를 바인딩한
    안정적인 고유 Key(FACIL_T{idx}_{own_id})가 포함된
    facility_total_view를 생성/갱신합니다.

    own_id는 각 물리 테이블의 진짜 PRIMARY KEY(BIGSERIAL)이며, 소스(fid)가
    삭제/재배치/재사용되어도 own_id 자체는 절대 바뀌거나 재사용되지 않으므로
    total_id는 항상 동일한 레코드만을 가리킨다.
    (삭제된 피처는 물리 삭제 대신 use_yn='n' 처리되어 행이 유지된다.)
    """
    log("📊 [facility_total_view 갱신 시작]")
    conn = None
    try:
        conn = get_data_conn()
        conn.autocommit = True
        cur = conn.cursor()

        # 1. qfield 스키마 내 모든 물리 테이블 목록 조회
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (TARGET_SCHEMA,))
        tables = [r[0] for r in cur.fetchall()]

        if not tables:
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

            if "project_id" in cols or "geometry" in cols or "geom" in cols:
                table_col_map[t_name] = cols
                valid_tables.append(t_name)
                for c_lower, (c_orig, _) in cols.items():
                    if c_lower not in system_cols:
                        all_unique_cols.add(c_orig)

        if not valid_tables:
            log("⚠️ [뷰 생성 스킵] 동기화 대상 물리 테이블이 없습니다.")
            return

        # 3. 정렬된 통합 컬럼 목록 정의
        sorted_custom_cols = sorted(list(all_unique_cols))

        # 4. 각 물리 테이블별 SELECT 쿼리문 조합
        union_parts = []
        for idx, t_name in enumerate(valid_tables, start=1):
            t_cols = table_col_map[t_name]

            # 테이블별 10,000,000 단위 프리픽스
            table_prefix_num = idx * 10000000

            # 💡 [핵심] own_id(우리 DB가 발급한, 절대 재사용되지 않는 PK)를
            #    total_id에 직접 바인딩. 소스 fid는 재배치/재사용될 수 있어
            #    매칭에만 쓰고, 외부에 노출되는 키(total_id)는 own_id로만 만든다.
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

            # 사용자 정의 컬럼 매핑 (없으면 NULL)
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

        # 5. facility_total_view 생성 SQL 실행
        union_all_sql = "\n    UNION ALL\n".join(union_parts)
        create_view_sql = f"""
CREATE OR REPLACE VIEW {TARGET_SCHEMA}.facility_total_view AS
{union_all_sql};
"""
        cur.execute(f"DROP VIEW IF EXISTS {TARGET_SCHEMA}.facility_total_view CASCADE")
        cur.execute(create_view_sql)
        log(f"✅ [facility_total_view 갱신 완료] 총 {len(valid_tables)}개 물리 테이블 통합됨 (total_id: FACIL_T{{idx}}_{{own_id}})")

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
    GPKG 레이어를 읽으면서 원본 feature id(fid)를 'fid' 컬럼으로 보존한다.
    fid는 GeoPackage 표준상 안정적인 식별자로, QField/QGIS에서 기존 피처를
    수정해도 값이 바뀌지 않고, 삭제된 fid는 일반적으로 재사용되지 않는다.
    """
    try:
        gdf = gpd.read_file(gpkg_path, layer=layer_name, fid_as_index=True)
        gdf = gdf.reset_index()
        if "index" in gdf.columns and "fid" not in gdf.columns:
            gdf = gdf.rename(columns={"index": "fid"})
    except TypeError:
        # geopandas/pyogrio 버전이 fid_as_index를 지원하지 않는 경우 fallback
        gdf = gpd.read_file(gpkg_path, layer=layer_name)
        if "fid" not in gdf.columns:
            raise ValueError(
                f"'{layer_name}' 레이어에서 안정적인 fid를 확보할 수 없습니다. "
                "geopandas/pyogrio 버전을 업그레이드하세요."
            )
    return gdf


def _slugify_table_part(text_val: str, maxlen: int = 40) -> str:
    """
    테이블명 조각을 안전하고 '고정된' 형태로 변환한다.
    (파일명+레이어명) 기반이므로 다른 레이어가 이번 사이클에 비어 있어도
    이 레이어의 테이블명은 절대 흔들리지 않는다.
    """
    slug = re.sub(r"[^0-9a-zA-Z_]+", "_", text_val.strip().lower()).strip("_")
    if not slug:
        slug = "layer"
    if len(slug) > maxlen:
        h = hashlib.md5(text_val.encode("utf-8")).hexdigest()[:8]
        slug = f"{slug[:maxlen - 9]}_{h}"
    return slug


def soft_delete_all_active(table_name, schema):
    """
    레이어의 모든 피처가 삭제되어 이번 사이클에 gdf가 비어있는 경우,
    (기존 테이블이 있다면) 그 테이블의 활성 행 전체를 소프트 삭제 처리한다.
    이렇게 해야 '레이어 전체 삭제'도 실제로 반영되고, 다른 레이어가 이 테이블
    자리를 대신 차지하는 일도 없다(테이블명이 고정이므로).
    """
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
            return  # 이 레이어로 저장된 적 자체가 없으면 할 일 없음

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
    """프로젝트 폴더 내 모든 GPKG 레이어를 읽어 PostGIS 테이블로 적재"""
    log(f"🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    clean_owner = owner.lower().replace(" ", "_").replace("-", "_")

    if not os.path.exists(project_path):
        return False

    gpkg_files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]
    if not gpkg_files:
        return False

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

            # 💡 [핵심 수정] 테이블명을 "몇 번째로 성공 처리됐는가"(순번)가 아니라
            #    (파일명+레이어명) 기반의 고정된 값으로 결정한다.
            #    예전 방식은 어떤 레이어가 이번 사이클에 완전히 비어버리면(전체 삭제 등)
            #    그 레이어를 건너뛰면서 뒤에 오는 다른 레이어가 그 순번(=테이블)을
            #    대신 차지했고, 그 결과 서로 무관한 레이어의 기존 데이터가
            #    "이번 사이클에 없는 src_key"로 오인되어 잘못 소프트 삭제되는 버그가 있었다.
            #    고정 테이블명을 쓰면 다른 레이어의 상태와 무관하게 항상 같은 테이블에만
            #    저장되므로 이 문제가 원천적으로 사라진다.
            layer_key = _slugify_table_part(f"{file_stem}_{layer_name}")
            table_name = f"{clean_owner}_{short_id}_{layer_key}"

            try:
                gdf = _read_gpkg_layer_with_fid(gpkg_path, layer_name)

                if gdf.empty:
                    # 레이어의 모든 피처가 삭제된 경우: 기존 테이블이 있다면
                    # 활성 행을 전부 소프트 삭제 처리해서 삭제를 실제로 반영한다.
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

                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path)
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
    QFieldCloud에서 삭제된 프로젝트 정리
    1) DB 물리 테이블의 use_yn='n' 소프트 삭제
    2) 로컬 디렉토리 캐시 삭제
    """
    if not current_project_ids:
        return False

    conn = None
    deleted_any = False
    try:
        conn = get_data_conn()
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """, (TARGET_SCHEMA,))
        tables = [r[0] for r in cur.fetchall()]

        for t_name in tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = 'project_id'
                )
            """, (TARGET_SCHEMA, t_name))

            if not cur.fetchone()[0]:
                continue

            cur.execute(f"""
                SELECT DISTINCT project_id FROM {TARGET_SCHEMA}."{t_name}"
                WHERE use_yn = 'y' AND project_id IS NOT NULL LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                tbl_pid = row[0]
                if tbl_pid not in current_project_ids:
                    cur.execute(f"""
                        UPDATE {TARGET_SCHEMA}."{t_name}"
                        SET use_yn = 'n', update_at = NOW()
                        WHERE use_yn = 'y'
                    """)
                    log(f"🧹 [소프트 삭제 완료] 테이블: {t_name} (프로젝트 ID: {tbl_pid})")
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