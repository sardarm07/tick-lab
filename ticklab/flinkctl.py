"""Controls the Flink job through Flink's REST API.

- `start` submits the core job unless it is running, resuming from the stack's last savepoint
  when there is one, so `make down` and `make up` lose no state.
- `stop` stops the job with a savepoint (state and Kafka offsets kept, nothing emitted early).
- `drain` stops it with `--drain`: event time jumps to the end, so every open window and timer
  fires. Replays and tests end this way, because a bounded input's last minutes would otherwise
  never close.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import httpx

from ticklab.settings import ROOT

log = logging.getLogger(__name__)

JOB_NAME = "ticklab-core"
ENTRY_CLASS = "ticklab.flink.CoreJob"
JAR = ROOT / "flink" / "target" / "ticklab-flink.jar"
SAVEPOINTS = "file:///flink-data/savepoints"


class Flink:
    def __init__(self, url: str = "http://127.0.0.1:8081", stack: str = "live") -> None:
        self._client = httpx.Client(base_url=url, timeout=60)
        self._last_savepoint = ROOT / "data" / "flink" / stack / "last-savepoint"

    def jobs(self) -> list[dict]:
        response = self._client.get("/jobs/overview")
        response.raise_for_status()
        return response.json()["jobs"]

    def running(self) -> dict | None:
        for job in self.jobs():
            if job["name"] == JOB_NAME and job["state"] in ("RUNNING", "CREATED", "RESTARTING"):
                return job
        return None

    def start(self, args: list[str] | None = None) -> str:
        if job := self.running():
            log.info("%s already running (%s)", JOB_NAME, job["jid"])
            return job["jid"]
        jar_id = self._upload(JAR)
        body: dict = {"entryClass": ENTRY_CLASS, "programArgsList": args or []}
        if self._last_savepoint.exists():
            body["savepointPath"] = self._last_savepoint.read_text().strip()
            log.info("resuming from %s", body["savepointPath"])
        response = self._client.post(f"/jars/{jar_id}/run", json=body)
        if response.status_code != 200:
            raise RuntimeError(f"submit failed: {response.text}")
        jid = response.json()["jobid"]
        self._wait_state(jid, {"RUNNING"})
        self._last_savepoint.unlink(missing_ok=True)  # used; the next stop writes a new one
        log.info("%s running (%s)", JOB_NAME, jid)
        return jid

    def stop(self, drain: bool) -> str | None:
        job = self.running()
        if job is None:
            log.info("%s is not running", JOB_NAME)
            return None
        response = self._client.post(
            f"/jobs/{job['jid']}/stop", json={"drain": drain, "targetDirectory": SAVEPOINTS}
        )
        response.raise_for_status()
        trigger = response.json()["request-id"]
        location = self._wait_savepoint(job["jid"], trigger)
        self._wait_state(job["jid"], {"FINISHED"})
        if drain:
            log.info("%s drained and stopped", JOB_NAME)
        else:
            self._last_savepoint.parent.mkdir(parents=True, exist_ok=True)
            self._last_savepoint.write_text(location + "\n")
            log.info("%s stopped with savepoint %s", JOB_NAME, location)
        return location

    def operator_counter(self, name: str) -> int:
        """A counter summed over every operator and subtask of the running job, such as
        "duplicates" from the deduplication operator."""
        job = self.running()
        if job is None:
            return 0
        response = self._client.get(f"/jobs/{job['jid']}")
        response.raise_for_status()
        vertices = [vertex["id"] for vertex in response.json()["vertices"]]
        # The REST API fetches metrics from the TaskManagers lazily: the first request can come
        # back empty and only start the fetch, so ask again for a few seconds.
        for _ in range(10):
            found, total = False, 0
            for vertex in vertices:
                base = f"/jobs/{job['jid']}/vertices/{vertex}/subtasks/metrics"
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
