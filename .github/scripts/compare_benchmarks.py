#!/usr/bin/env python3
"""把两份 bench_* 的输出按行名对齐，比较 ns/op；超过阈值的行发 GitHub warning 注解（不让作业失败）。

共享 runner 每次可能是不同型号、不同邻居的 VM：同一提交重跑一次，纯用户态的基准就能慢 20–40%。所以不直接比
绝对 ns，而是用同一次运行里的外部参照——Asio 与 libuv 的行，它们的代码不随本库的提交变化——估出"这台机器
慢了多少"再扣掉。机器对不同场景的影响差别很大（换到 E 核：协程 / post 慢 1.5 倍、TCP 往返慢 2.3 倍、
定时器慢 1.3 倍），所以按场景分组配对参照：

    rtt         TCP 往返              ↔ Asio / libuv 的 tcp echo round trip
    throughput  TCP 吞吐              ↔ Asio / libuv 的 throughput
    connect     connect + accept      ↔ Asio / libuv 的 connect + accept
    timer       定时器到期 + 恢复     ↔ Asio 的 timer expire（libuv 的 0 ms 定时器是下一圈循环，不可比）
    cpu         协程 / 执行器 / 擦除  ↔ Asio 的 co_await child / co_spawn / post hop

归一化变化 = (current / previous) / 该组参照的中位比值 − 1。两类行只展示不报警：跨线程的行（32 连接 × 4 线程、
thread_pool 的 hop——取决于 runner 的核数与调度，同一份库代码两次运行能差一倍以上）和 TLS 行（主要是 OpenSSL
的加解密，这个作业里没有能扣掉加密速度的参照）。

报警规则：同一提交不报警（差异全是 runner 噪声，表格就是噪声底）；两次运行 CPU 型号相同时归一化变化超过
--threshold（默认 15%）报警，型号不同或未知时超过 --cross-machine-threshold（默认 25%）才报警。

结果文件开头的 "# sha=" / "# cpu=" 两行由 CI 的 Run 步骤写入；旧 artifact 没有它们时按"未知机器"处理。CI 把整套
跑三遍写进同一个文件，同名行取最小值（噪声是单向的，只会变慢）：单遍时同一颗核上 TCP 往返行两次运行能差 17%。

用法：compare_benchmarks.py previous.txt current.txt [--threshold 0.15] [--cross-machine-threshold 0.25]
"""

import argparse
import re
import statistics
import sys
from pathlib import Path

# 表行：名字（可能超过 46 列、含空格）、ns/op（一位小数）、allocs/op（三位小数）、usr ns、sys ns、iters、note
ROW = re.compile(r"^(?P<name>.+?)\s+(?P<ns>-?\d+\.\d)\s+(?P<allocs>-?\d+\.\d{3})\s+(?P<usr>-?\d+)\s+(?P<sys>-?\d+)\s+(?P<iters>\d+)\s*(?P<note>.*)$")
# 旧格式（没有 usr / sys 两列）也接受。
ROW_OLD = re.compile(r"^(?P<name>.+?)\s+(?P<ns>-?\d+\.\d)\s+(?P<allocs>-?\d+\.\d{3})\s+(?P<iters>\d+)\s*(?P<note>.*)$")
META = re.compile(r"^#\s*(?P<key>[a-z]+)=(?P<value>.*)$")

GROUPS = ("rtt", "throughput", "connect", "timer", "cpu")


def parse(path: Path):
    meta, rows = {}, {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = META.match(line)
        if m:
            meta[m.group("key")] = m.group("value").strip()
            continue
        m = ROW.match(line) or ROW_OLD.match(line)
        if not m:
            continue
        name = m.group("name").strip()
        if name in ("benchmark", "---------"):
            continue
        # 同一行出现多次（CI 把整套跑几遍）取最小值：噪声只会让它变慢。
        ns = float(m.group("ns"))
        rows[name] = min(ns, rows[name]) if name in rows else ns
    return meta, rows


def is_reference(name: str) -> bool:
    return name.startswith("asio ") or name.startswith("libuv:")


def group_of(name: str):
    """行所属的场景组；None 表示只展示、不参与报警。"""
    if "threads" in name or "thread_pool" in name or "tls" in name:
        return None
    if "connect + accept" in name:
        return "connect"
    if "throughput" in name:
        return "throughput"
    if "timer expire" in name:
        return "timer"
    if "round trip" in name and "tcp" in name:
        return "rtt"
    return "cpu"


def reference_group_of(name: str):
    if name.startswith("libuv:") and ("0 ms timer" in name or "idle hop" in name):
        return None  # 语义与本库的行不同：下一圈循环，不是 1 µs timerfd / 跨线程 post
    return group_of(name)


def machine_factors(previous: dict, current: dict) -> dict:
    factors = {}
    for g in GROUPS:
        ratios = [current[n] / previous[n] for n in previous
                  if is_reference(n) and n in current and previous[n] > 0.0 and reference_group_of(n) == g]
        factors[g] = (statistics.median(ratios), len(ratios)) if ratios else (None, 0)
    return factors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("previous", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--threshold", type=float, default=0.15, help="同一 CPU 型号时的归一化退化阈值")
    parser.add_argument("--cross-machine-threshold", type=float, default=0.25, help="CPU 型号不同或未知时的归一化退化阈值")
    args = parser.parse_args()

    current_meta, current = parse(args.current)
    if not args.previous.exists():
        print("## Benchmark comparison\n\n没有上一次的结果可比（首次运行或 artifact 已过期）。\n")
        return 0
    previous_meta, previous = parse(args.previous)

    same_commit = bool(current_meta.get("sha")) and current_meta.get("sha") == previous_meta.get("sha")
    same_cpu = bool(current_meta.get("cpu")) and current_meta.get("cpu") == previous_meta.get("cpu")
    threshold = args.threshold if same_cpu else args.cross_machine_threshold
    factors = machine_factors(previous, current)

    print("## Benchmark comparison (ns/op, quick mode, shared runner)\n")
    print(f"- previous: `{previous_meta.get('sha', '?')[:10]}` on {previous_meta.get('cpu', 'unknown CPU')}")
    print(f"- current: `{current_meta.get('sha', '?')[:10]}` on {current_meta.get('cpu', 'unknown CPU')}")
    factor_text = ", ".join(f"{g} ×{f:.2f} (n={n})" if f else f"{g} —" for g, (f, n) in factors.items())
    print(f"- 机器系数（同一次运行里 Asio / libuv 参照行的中位比值）：{factor_text}")
    if same_commit:
        print("- **同一提交重跑**：差异全是 runner 噪声，不报警；这张表就是这台 runner 的噪声底。")
    else:
        print(f"- 报警阈值：归一化变化 > +{threshold:.0%}（{'CPU 型号相同' if same_cpu else 'CPU 型号不同或未知'}）。多线程与 TLS 行只展示。")
    print()
    print("| benchmark | group | previous | current | raw | normalized |")
    print("| --- | --- | ---: | ---: | ---: | ---: |")

    regressions = []
    for name, now in current.items():
        before = previous.get(name)
        if before is None or before <= 0.0:
            print(f"| {name} | | — | {now:.1f} | new | |")
            continue
        raw = now / before - 1.0
        if is_reference(name):
            print(f"| {name} | reference | {before:.1f} | {now:.1f} | {raw:+.1%} | |")
            continue
        group = group_of(name)
        factor = factors[group][0] if group else None
        if group is None:
            print(f"| {name} | info | {before:.1f} | {now:.1f} | {raw:+.1%} | |")
            continue
        normalized = now / before / (factor if factor else 1.0) - 1.0
        flagged = not same_commit and normalized > threshold and raw > threshold
        marker = " ⚠" if flagged else ""
        print(f"| {name} | {group} | {before:.1f} | {now:.1f} | {raw:+.1%} | {normalized:+.1%}{marker} |")
        if flagged:
            regressions.append((name, before, now, raw, normalized, group, factor))
    for name in previous:
        if name not in current:
            print(f"| {name} | | {previous[name]:.1f} | — | removed | |")
    print()

    for name, before, now, raw, normalized, group, factor in regressions:
        factor_note = f"machine ×{factor:.2f} for {group}" if factor else f"no {group} reference"
        sys.stderr.write(f"::warning title=Benchmark regression::{name}: {before:.1f} -> {now:.1f} ns/op "
                         f"(raw {raw:+.1%}, normalized {normalized:+.1%}, {factor_note})\n")
    if same_commit:
        print("同一提交，不做回归判断。\n")
    elif regressions:
        print(f"**{len(regressions)} 行归一化后退化超过 {threshold:.0%}**（见 warning 注解）。单次比较仍可能是噪声，下一次运行还在才值得追。\n")
    else:
        print("没有超过阈值的退化。\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
