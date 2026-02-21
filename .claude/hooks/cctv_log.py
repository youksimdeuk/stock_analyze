"""
CCTV Hook: PostToolUse 시 수정된 파일명을 modification_log.txt에 기록
Claude Code가 stdin으로 JSON을 전달함
"""
import sys
import json
import datetime

LOG_PATH = "c:/dev/기업분석/.claude/modification_log.txt"

try:
    data = json.load(sys.stdin)
    tool_name = data.get("tool_name", "?")
    tool_input = data.get("tool_input", {})
    file_path = tool_input.get("file_path", "?")
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[CCTV] {ts} | {tool_name} | {file_path}\n"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)
except Exception:
    pass  # 로그 실패 시 조용히 무시 (작업 흐름 방해 방지)
