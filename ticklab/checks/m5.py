"""`make check-m5 PROFILE=lite|full`: the benchmarks of milestone M5, with their evidence.

One profile per run, because switching between them means switching the codespace's machine
(2 cores and 8 GB for lite, 4 cores and 16 GB for full), which restarts it:

1. the busiest hour, replayed at 1x, 10x and 100x on the profile, each into an empty replay
   stack: every trade delivered, and the results written to bench/results/;
2. (lite only) Prometheus scrapes every service that serves metrics, and every panel of every
   Grafana dashboard shows data from the live stack, after it has run for 10 minutes;
3. (lite only) the README's Mermaid diagram renders with the Mermaid CLI.

About 1.5 hours per profile, most of it the 1x replay. `--skip-bench` runs only checks 2 and 3,
after a run whose benchmarks passed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time

import httpx

from ticklab.checks.common import Report, disk_line, make, prometheus, say, stamp
from ticklab.settings import ROOT

GRAFANA_URL = "http://127.0.0.1:3000"
DASHBOARDS = ROOT / "infra" / "grafana" / "dashboards"
# Every service that serves metrics, by its job in infra/prometheus/prometheus.yml.
SCRAPED = (
    "producer-binance",
    "producer-kraken",
    "reconciler",
    "gap-repair",
    "flink",
    "redpanda",
    "clickhouse",
)

SPEEDS = ("1", "10", "100")
MERMAID_IMAGE = "minlag/mermaid-cli:12.0.1"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=["lite", "full"], required=True)
    parser.add_argument("--skip-bench", action="store_true", help="only checks 2 and 3")
    args = parser.parse_args()
    report = Report(f"M5, {args.profile}")
    say(f"# M5 check, {args.profile} profile, started {stamp()} UTC\n\ndisk: {disk_line()}\n")
    if not args.skip_bench:
        make("down")
        for speed in SPEEDS:
            make("bench", SPEED=speed, PROFILE=args.profile)
            path = ROOT / "bench" / "results" / f"{args.profile}-{speed}x.json"
            result = json.loads(path.read_text())
            say((path.with_suffix(".md")).read_text())
            report.record(
                f"1. {args.profile} at {speed}x: every trade delivered and measured",
                result["undelivered"] == 0
                and result["failed"] == 0
                and result["output_latency_ms"]["n"] > 0,
                f"({result['trades']:,} trades, {result['trades_per_second']:,} a second)",
            )
        make("replay-down", SIZE=args.profile)
    if args.profile == "lite":
        make("up")
        say(f"[{stamp()}] live for 10 minutes, so every dashboard has data")
        time.sleep(600)
        check_dashboards(report)
        make("down")
        check_mermaid(report)
    say(f"\ndisk: {disk_line()}")
    return report.finish()


def check_dashboards(report: Report) -> None:
    """Every panel's queries, through Grafana's own query API, over the last 15 minutes. Every
    query must succeed, and every chart and figure must show data: some of a panel's queries
    may rightly return nothing, such as reconnects when there were none, but not all of them.
    Tables of events (alerts, gaps, silences, mismatches) may rightly be empty when nothing has
    happened. A figure that falls back to 0 (`or vector(0)`) would hide a service whose metrics
    never arrive, so Prometheus's own `up` must be 1 for every service that serves them."""
    up: dict[str, float] = {}
    for row in prometheus("up"):
        job = row["metric"].get("job", "?")
        up[job] = min(up.get(job, 1.0), float(row["value"][1]))
    unscraped = [job for job in SCRAPED if up.get(job) != 1.0]
    failed, empty, no_rows = [], [], []
    count = 0
    for path in sorted(DASHBOARDS.glob("*.json")):
        dashboard = json.loads(path.read_text())
        for panel in dashboard["panels"]:
            name = f"{dashboard['title']}: {panel['title']}"
            count += len(panel["targets"])
            points, errors = panel_data(dashboard, panel, "now-15m", "now")
            failed += [f"{name} {error}" for error in errors]
            if points == 0:
                (no_rows if panel["type"] == "table" else empty).append(name)
    for job in unscraped:
        say(f"- not scraped: {job} (up = {up.get(job, 'no series')})")
    for line in failed:
        say(f"- failed: {line}")
    for line in empty:
        say(f"- no data: {line}")
    for line in no_rows:
        say(f"- an empty table (nothing of the kind happened): {line}")
    report.record(
        "2. every service is scraped, every dashboard query succeeds, and every panel has data",
        not unscraped and not failed and not empty,
        f"({len(SCRAPED)} services, {count} queries, {len(no_rows)} empty event tables)",
    )


def panel_data(dashboard: dict, panel: dict, start: str, end: str) -> tuple[int, list[str]]:
    """The points a panel's queries return through Grafana's own query API, between `start` and
    `end` (Grafana's time strings, such as now-15m or milliseconds since the epoch), and the
    errors of those that fail."""
    auth = ("admin", read_env("GRAFANA_ADMIN_PASSWORD", "admin"))
    variables = {
        v["name"]: v["current"]["value"] for v in dashboard.get("templating", {}).get("list", [])
    }
    points, errors = 0, []
    for target in panel["targets"]:
        query = dict(target, intervalMs=10_000, maxDataPoints=100)
        if "rawSql" in query:
            for variable, value in variables.items():
                query["rawSql"] = query["rawSql"].replace("${" + variable + "}", value)
        body = {"from": start, "to": end, "queries": [query]}
        response = httpx.post(f"{GRAFANA_URL}/api/ds/query", json=body, auth=auth, timeout=60)
        result = response.json().get("results", {}).get(target["refId"], {})
        points += sum(
            len(frame["data"]["values"][0])
            for frame in result.get("frames", [])
            if frame["data"]["values"]
        )
        if response.status_code != 200 or result.get("error"):
            errors.append(f"[{target['refId']}]: {result.get('error') or response.status_code}")
    return points, errors


def read_env(name: str, default: str) -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1]
    return default


def check_mermaid(report: Report) -> None:
    """Renders the first Mermaid block of the README to SVG, in Docker."""
    readme = (ROOT / "README.md").read_text()
    match = re.search(r"```mermaid\n(.*?)```", readme, re.S)
    if not match:
        report.record("3. the README's Mermaid diagram renders", False, "(no diagram found)")
        return
    out = ROOT / "data" / "checks" / "m5"
    out.mkdir(parents=True, exist_ok=True)
    (out / "architecture.mmd").write_text(match.group(1))
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-u",
            "1000",
            "-v",
            f"{out}:/data",
            MERMAID_IMAGE,
            "-i",
            "/data/architecture.mmd",
            "-o",
            "/data/architecture.svg",
        ],
        capture_output=True,
        text=True,
    )
    svg = out / "architecture.svg"
    ok = result.returncode == 0 and svg.exists() and svg.stat().st_size > 1000
    report.record(
        "3. the README's Mermaid diagram renders",
        ok,
        f"({svg.stat().st_size:,} bytes of SVG)" if ok else result.stderr[-300:],
    )


if __name__ == "__main__":
    raise SystemExit(main())
