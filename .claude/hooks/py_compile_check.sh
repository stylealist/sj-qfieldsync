#!/usr/bin/env bash
# PostToolUse(Edit|Write) 훅: 수정된 파일이 .py면 python -m py_compile로 문법만 즉시 검증한다.
# 이 저장소에는 별도 빌드/린트/테스트 시스템이 없어, 이 훅이 유일한 자동 검증 수단이다.
f=$(jq -r '.tool_input.file_path // empty')
case "$f" in
  *.py) python -m py_compile "$f" ;;
esac
