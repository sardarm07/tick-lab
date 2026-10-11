"""Controls the Flink jobs through Flink's REST API.

There are two jobs, `core` (candles, gaps, feed health) and `signals` (spreads and alerts),
in one jar on one session cluster.

- `start` submits a job unless it is running, resuming from the stack's last savepoint of that
  job when there is one, so `make down` and `make up` lose no state.
- `stop` stops a job with a savepoint (state and Kafka offsets kept, nothing emitted early).
- `drain` stops it with `--drain`: event time jumps to the end, so every open window and timer
  fires. Replays and tests end this way, because a bounded input's last minutes would otherwise
  never close. It first waits until the job has read everything in Kafka: stopping also stops
  the source where it is, and a job still behind its input would never see the rest.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from ticklab.settings import ROOT

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    key: str
    name: str
    entry_class: str


CORE = Job("core", "ticklab-core", "ticklab.flink.CoreJob")
SIGNALS = Job("signals", "ticklab-signals", "ticklab.flink.SignalsJob")
JOBS = (CORE, SIGNALS)
JAR = ROOT / "flink" / "target" / "ticklab-flink.jar"
SAVEPOINTS = "file:///flink-data/savepoints"


class Flink:
    def __init__(self, url: str = "http://127.0.0.1:8081", stack: str = "live") -> None:
        self._client = httpx.Client(base_url=url, timeout=60)
        self._savepoints = ROOT / "data" / "flink" / stack

    def _last_savepoint(self, job: Job) -> Path:
        return self._savepoints / f"{job.key}.savepoint"

    def jobs(self) -> list[dict]:
        response = self._client.get("/jobs/overview")
        response.raise_for_status()
        return response.json()["jobs"]

    def running(self, job: Job = CORE) -> dict | None:
        for found in self.jobs():
            if found["name"] == job.name and found["state"] in ("RUNNING", "CREATED", "RESTARTING"):
                return found
        return None

    def start(self, job: Job, args: list[str] | None = None) -> str:
        if found := self.running(job):
            log.info("%s already running (%s)", job.name, found["jid"])
            return found["jid"]
        jar_id = self._upload(JAR)
        body: dict = {"entryClass": job.entry_class, "programArgsList": args or []}
        last = self._last_savepoint(job)
        if last.exists():
            body["savepointPath"] = last.read_text().strip()
            log.info("%s: resuming from %s", job.name, body["savepointPath"])
        response = self._client.post(f"/jars/{jar_id}/run", json=body)
        if response.status_code != 200:
            raise RuntimeError(f"submit failed: {response.text}")
        jid = response.json()["jobid"]
        self._wait_state(jid, {"RUNNING"})
        last.unlink(missing_ok=True)  # used; the next stop writes a new one
        log.info("%s running (%s)", job.name, jid)
        return jid

    def stop(self, job: Job, drain: bool) -> str | None:
        found = self.running(job)
        if found is None:
            log.info("%s is not running", job.name)
            return None
        if drain:
            self._wait_caught_up(job)
        response = self._client.post(
            f"/jobs/{found['jid']}/stop", json={"drain": drain, "targetDirectory": SAVEPOINTS}
        )
        response.raise_for_status()
        trigger = response.json()["request-id"]
        location = self._wait_savepoint(found["jid"], trigger)
        self._wait_state(found["jid"], {"FINISHED"})
        if drain:
            log.info("%s drained and stopped", job.name)
        else:
            last = self._last_savepoint(job)
            last.parent.mkdir(parents=True, exist_ok=True)
            last.write_text(location + "\n")
            log.info("%s stopped with savepoint %s", job.name, location)
        return location

    def operator_counter(self, name: str, job: Job = CORE) -> int:
        """A counter summed over every operator and subtask of a running job, such as
        "duplicates" from the deduplication operator."""
        active = self.running(job)
        if active is None:
            return 0
        response = self._client.get(f"/jobs/{active['jid']}")
        response.raise_for_status()
        vertices = [vertex["id"] for vertex in response.json()["vertices"]]
        # The REST API fetches metrics from the TaskManagers lazily: the first request can come
        # back empty and only start the fetch, so ask again for a few seconds.
        for _ in range(10):
            found, total = False, 0
            for vertex in vertices:
                base = f"/jobs/{active['jid']}/vertices/{vertex}/subtasks/metrics"
                available = self._client.get(base).json()
                ids = [m["id"] for m in available if m["id"].endswith("." + name)]
                if ids:
                    found = True
                    values = self._client.get(base, params={"get": ",".join(ids)}).json()
                    total += sum(int(float(v.get("sum", 0))) for v in values)
            if found:
                return total
            time.sleep(2)
        raise RuntimeError(f"no operator reports a metric named {name!r}")

    def _wait_caught_up(self, job: Job, timeout_s: float = 3600) -> None:
        """Until the job's Kafka source has nothing left to read (its pendingRecords)."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                pending = self.operator_counter("pendingRecords", job)
            except ValueError:  # not known yet: the metric reads NaN at first
                pending = -1
            if pending == 0:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"{job.name}: {pending:,} records still unread")
            log.info("%s: %s records still to read", job.name, f"{pending:,}")
            time.sleep(5)

    def _upload(self, jar: Path) -> str:
        with open(jar, "rb") as f:
            response = self._client.post(
                "/jars/upload", files={"jarfile": (jar.name, f, "application/x-java-archive")}
            )
        response.raise_for_status()
        return response.json()["filename"].rsplit("/", 1)[-1]

    def _wait_savepoint(self, jid: str, trigger: str, timeout_s: float = 300) -> str:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            response = self._client.get(f"/jobs/{jid}/savepoints/{trigger}")
            response.raise_for_status()
            body = response.json()
            if body["status"]["id"] == "COMPLETED":
                operation = body["operation"]
                if "failure-cause" in operation:
                    raise RuntimeError(f"savepoint failed: {operation['failure-cause']}")
                return operation["location"]
            time.sleep(1)
        raise TimeoutError("savepoint did not complete")

    def _wait_state(self, jid: str, states: set[str], timeout_s: float = 180) -> None:
        deadline = time.monotonic() + timeout_s
        state = "?"
        while time.monotonic() < deadline:
            response = self._client.get(f"/jobs/{jid}")
            response.raise_for_status()
            state = response.json()["state"]
            if state in states:
                return
            if state in ("FAILED", "CANCELED"):
                raise RuntimeError(f"job {jid} is {state}")
            time.sleep(1)
        raise TimeoutError(f"job {jid} stayed {state}")
