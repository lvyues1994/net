#!/usr/bin/env python3
"""把 CTest 的 LastTest.log 里失败测试的输出末尾变成工作流注解（::error）。

注解经 GitHub 的 check-runs API 对公开仓库匿名可读，不需要日志的管理员权限——这是在没有
Windows 机器的情况下拿到 MSVC 作业失败原因的通道。"""

import re
import sys

TAIL_LINES = 40


def sections(text):
    # 每个测试块以 "N/M Testing: name" 开头，以 "Test time = ..." 结束。
    parts = re.split(r"^\d+/\d+ Testing: (.+)$", text, flags=re.M)
    for i in range(1, len(parts) - 1, 2):
        yield parts[i].strip(), parts[i + 1]


def main(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError as e:
        print(f"::warning::cannot read {path}: {e}")
        return 0
    failed = 0
    for name, body in sections(text):
        if re.search(r"^Test (Passed|Skipped)\.$", body, flags=re.M):
            continue
        if not re.search(r"^Test (Failed|Timeout)\.?", body, flags=re.M) and "Test Failed" not in body:
            continue
        failed += 1
        lines = [l for l in body.splitlines() if l.strip()]
        tail = lines[-TAIL_LINES:]
        message = "%0A".join(l.replace("%", "%25") for l in tail)
        print(f"::error title=ctest failed: {name}::{message}")
    if failed == 0:
        print("::notice::no failed test sections found in LastTest.log")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "build/Testing/Temporary/LastTest.log"))
