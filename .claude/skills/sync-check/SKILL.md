---
name: sync-check
description: sj-qfieldsync 저장소에서 qfield_data_sync.py를 수정한 뒤, 커밋하기 전에 실행하는 자동 점검. 문법 검증 + 이 저장소의 핵심 불변식(소프트 삭제 관례, total_seq/total_id 불변성, 하드코딩 자격증명) 관련 위험 패턴을 grep으로 훑는다.
---

이 저장소에는 빌드/린트/테스트 시스템이 없으므로, 커밋 전에 아래 절차로 빠르게 자체 점검한다. 각 단계 결과를 사용자에게 간단히 요약해서 보고한다.

## 1. 문법 검증

```bash
python -m py_compile qfield_data_sync.py
```

실패하면 즉시 원인을 고치고 다시 실행한다.

## 2. 소프트 삭제 관례 위반 검사

물리 `DELETE` 문이 새로 추가되지 않았는지 확인한다(`archive_and_drop_table`의 `DROP TABLE`은 정상 — 아카이브 이관 후에만 허용됨).

```bash
grep -n "DELETE FROM" qfield_data_sync.py
```

`facility_deleted_archive`나 위 함수와 무관한 위치에서 `DELETE FROM`이 새로 나타났다면 소프트 삭제(`use_yn='n'`) 패턴으로 바꿀 것을 제안한다.

## 3. total_seq / total_id 관련 함수 변경 여부 확인

다음 함수들이 diff에 포함되어 있다면, `facility_total_view`의 `total_seq`/`total_id`가 삭제 전후로 절대 바뀌지 않는다는 불변식이 깨지지 않았는지 특히 주의 깊게 직접 확인한다:

- `ensure_table_idx_registry` (idx 영구 고정)
- `archive_and_drop_table` (`orig_total_seq`/`orig_total_id` 고정 저장)
- `update_facility_total_view` (`table_prefix_num = idx * 10000000` 계산식)

```bash
git diff -- qfield_data_sync.py | grep -n "^[+-].*\(ensure_table_idx_registry\|orig_total_seq\|orig_total_id\|table_prefix_num\)"
```

## 4. 하드코딩 자격증명 diff 검사

새로운 비밀번호/토큰이 diff에 추가되지 않았는지 확인한다(기존 `QFC_PASSWORD`/`DATA_DB` 항목은 이미 알려진 상태이며 새로 추가되는 것만 문제).

```bash
git diff -- qfield_data_sync.py | grep -niE "^\+.*(password|secret|token|api_key)\s*="
```

## 5. 요약 및 다음 단계

위 4개 점검을 통과했으면, 더 깊은 로직 검토가 필요할 때 `reviewer` 서브에이전트로 리뷰를 넘길 것을 제안한다(특히 3번 항목에 해당 함수가 걸린 경우).
