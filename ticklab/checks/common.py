"""Helpers shared by the milestone checks."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ticklab.settings import ROOT

PROMETHEUS_URL = "http://127.0.0.1:9090"
OUT_DIR = ROOT / "data" / "checks"


def say(text: str = "") -> None:
    print(text, flush=True)


def stamp() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def make(*targets: str, **variables: str) -> None:
    command = ["make", *targets, *(f"{k}={v}" for k, v in variables.items())]
    say(f"$ {' '.join(command)}")
    subprocess.run(command, cwd=ROOT, check=True)


def run(command: list[str], check: bool = True) -> str:
    return subprocess.run(command, cwd=ROOT, check=check, capture_output=True, text=True).stdout


def wait(minutes: float, label: str, every_s: float = 60, report=None) -> None:
    """Sleep, printing a progress line every minute (it also keeps the session active)."""
    end = time.monotonic() + minutes * 60
    while (left := end - time.monotonic()) > 0:
        time.sleep(min(every_s, left))
        extra = f": {report()}" if report else ""
        say(f"  [{stamp()}] {label}, {max(0.0, end - time.monotonic()) / 60:.1f} min left{extra}")


_UNITS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "kB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
}


def _bytes(text: str) -> float:
    match = re.fullmatch(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    if not match:
        raise ValueError(text)
    return float(match.group(1)) * _UNITS[match.group(2)]


@dataclass
class ContainerState:
    name: str
    health: str
    memory_mib: float
    limit_mib: float


def containers(project: str) -> list[ContainerState]:
    names = run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Names}}",
        ]
    ).split()
    if not names:
        return []
    stats = {}
    for line in run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names]
    ).splitlines():
        row = json.loads(line)
        used, limit = row["MemUsage"].split("/")
        stats[row["Name"]] = (_bytes(used) / 2**20, _bytes(limit) / 2**20)
    states = []
    for name in sorted(names):
        health = run(
            [
                "docker",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                name,
            ]
        ).strip()
        used, limit = stats[name]
        states.append(ContainerState(name, health, used, limit))
    return states


def disk_line() -> str:
    lines = run(["df", "-h", "/"]).splitlines()
    return lines[-1] if lines else "?"


def prometheus(query: str) -> list[dict]:
    response = httpx.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=30)
    response.raise_for_status()
    return response.json()["data"]["result"]


def micros(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def iso(us: int) -> str:
    return datetime.fromtimestamp(us / 1e6, UTC).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Report:
    name: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def record(self, check: str, passed: bool, detail: str) -> None:
        self.results.append((check, passed, detail))
        say(f"\n==> {check}: {'PASS' if passed else 'FAIL'} {detail}\n")

    def finish(self) -> int:
        say(f"\n## {self.name} summary\n")
        for check, passed, detail in self.results:
            say(f"- {'PASS' if passed else 'FAIL'}  {check}  {detail}")
        failed = [r for r in self.results if not r[1]]
        say(f"\n{len(self.results) - len(failed)} of {len(self.results)} checks passed.")
        return 1 if failed else 0


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def fail_fast(message: str) -> None:
    say(f"ERROR: {message}")
    sys.exit(2)
