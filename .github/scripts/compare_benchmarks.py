#!/usr/bin/env python3
"""把两份 bench_* 的输出按行名对齐，算 ns/op 的变化；超过阈值的行发 GitHub 注解（warning，不让作业失败：
共享 runner 的噪声 ±10% 是常态，这道门只负责把 +15% 以上的退化摆到眼前）。输出一张 Markdown 表给 step summary。

用法：compare_benchmarks.py previous.txt current.txt [--threshold 0.15]
"""

import argparse
import re
import sys
from pathlib import Path

# 表行：名字（可能超过 46 列、含空格）、ns/op（一位小数）、allocs/op（三位小数）、usr ns、sys ns、iters、note
ROW = re.compile(r"^(?P<name>.+?)\s+(?P<ns>-?\d+\.\d)\s+(?P<allocs>-?\d+\.\d{3})\s+(?P<usr>-?\d+)\s+(?P<sys>-?\d+)\s+(?P<iters>\d+)\s*(?P<note>.*)$")
# 旧格式（没有 usr / sys 两列）也接受，这样第一次启用时还能和上一版的 artifact 比。
ROW_OLD = re.compile(r"^(?P<name>.+?)\s+(?P<ns>-?\d+\.\d)\s+(?P<allocs>-?\d+\.\d{3})\s+(?P<iters>\d+)\s*(?P<note>.*)$")


def parse(path: Path) -> dict:
    rows = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = ROW.match(line) or ROW_OLD.match(line)
        if not match:
            continue
        name = match.group("name").strip()
        if name in ("benchmark", "---------"):
            continue
        rows[name] = float(match.group("ns"))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("previous", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--threshold", type=float, default=0.15, help="退化比例阈值（0.15 = 15%）")
    args = parser.parse_args()

    current = parse(args.current)
    if not args.previous.exists():
        print("## Benchmark comparison\n\n没有上一次的结果可比（首次运行或 artifact 已过期）。\n")
        return 0
    previous = parse(args.previous)

    print("## Benchmark comparison (ns/op, quick mode, shared runner)\n")
    print(f"退化阈值 +{args.threshold:.0%}；超过的行有 warning 注解。噪声 ±10% 是常态，连续两次超阈值才值得追。\n")
    print("| benchmark | previous | current | delta |")
    print("| --- | ---: | ---: | ---: |")
    regressions = []
    for name, now in current.items():
        before = previous.get(name)
        if before is None or before <= 0.0:
            print(f"| {name} | — | {now:.1f} | new |")
            continue
        delta = (now - before) / before
        marker = " ⚠" if delta > args.threshold else (" ✓" if delta < -args.threshold else "")
        print(f"| {name} | {before:.1f} | {now:.1f} | {delta:+.1%}{marker} |")
        if delta > args.threshold:
            regressions.append((name, before, now, delta))
    for name in previous:
        if name not in current:
            print(f"| {name} | {previous[name]:.1f} | — | removed |")
    print()
    for name, before, now, delta in regressions:
        # GitHub Actions 注解：出现在作业摘要与 PR 的 Checks 里。
        sys.stderr.write(f"::warning title=Benchmark regression::{name}: {before:.1f} -> {now:.1f} ns/op ({delta:+.1%})\n")
    if regressions:
        print(f"**{len(regressions)} 行退化超过 {args.threshold:.0%}**（见 warning 注解）。\n")
    else:
        print("没有超过阈值的退化。\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
