"""Subprocess runner for hot-capacity audit workloads on GPU workers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


def _root_hex(raw: object) -> str:
    if isinstance(raw, str):
        text = raw.strip()
        body = text[2:] if text.startswith("0x") else text
        if len(body) == 64:
            try:
                bytes.fromhex(body)
                return body
            except ValueError:
                pass
    if isinstance(raw, list) and raw:
        try:
            return bytes(raw).hex()
        except Exception:
            pass
    return str(raw or "").strip()


@dataclass
class AuditJobRecord:
    job_id: str
    lease_id: str
    audit_id: str
    out_dir: Path
    challenge_file: Path
    proc: Optional[subprocess.Popen] = None
    phase: str = "starting"
    error: str = ""
    pass0_root: str = ""
    final_timing: dict = field(default_factory=dict)
    final_summary: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    monitor_thread: Optional[threading.Thread] = None


class AuditJobRunner:
    def __init__(self) -> None:
        self._jobs: dict[str, AuditJobRecord] = {}
        self._lock = threading.Lock()

    def _workspace_script(self) -> Path:
        return Path(__file__).resolve().parents[2] / "scripts" / "hot_capacity_workspace" / "bench_combined.py"

    def _workspace_command(self, script: Path) -> list[str]:
        if script.exists():
            return [sys.executable, str(script)]
        return [
            sys.executable,
            "-c",
            "from hot_capacity_workspace.bench_combined import main; main()",
        ]

    def _workspace_build_env(self, script_dir: Path) -> dict[str, str]:
        env = os.environ.copy()
        if script_dir.exists():
            current_pythonpath = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = (
                f"{script_dir}:{current_pythonpath}" if current_pythonpath else str(script_dir)
            )
        try:
            import torch

            torch_lib = Path(torch.__file__).resolve().parent / "lib"
            if torch_lib.exists():
                current_ld_path = env.get("LD_LIBRARY_PATH", "")
                env["LD_LIBRARY_PATH"] = (
                    f"{torch_lib}:{current_ld_path}" if current_ld_path else str(torch_lib)
                )
        except Exception:
            pass
        env.setdefault("MAX_JOBS", "2")
        return env

    def _build_cmd(
        self,
        *,
        script: Path,
        record: AuditJobRecord,
        payload: dict[str, Any],
        proof_seed: str,
    ) -> list[str]:
        challenge_timeout_s = float(payload.get("challenge_timeout_s") or 120.0)
        cmd = [
            *self._workspace_command(script),
            "--child",
            "--out-dir", str(record.out_dir),
            "--lease-id", record.lease_id,
            "--gpu-index", "0",
            "--challenge-file", str(record.challenge_file),
            "--challenge-timeout-s", str(challenge_timeout_s),
        ]
        if proof_seed:
            cmd.extend(["--seed-hex", proof_seed])
        workload_spec = payload.get("workload_spec") if isinstance(payload.get("workload_spec"), dict) else {}
        for key, value in workload_spec.items():
            if key in {"workload_version", "pass_count"}:
                continue
            cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        return cmd

    def _monitor_job(self, record: AuditJobRecord) -> None:
        lease = record.lease_id
        out_dir = record.out_dir
        pass0_path = out_dir / f"{lease}_pass0.json"
        final_path = out_dir / f"{lease}_final_timing.json"
        final_summary_path = out_dir / f"{lease}_final.json"
        proc = record.proc
        if proc is None:
            with record.lock:
                record.phase = "failed"
                record.error = "process not started"
            return
        while proc.poll() is None:
            with record.lock:
                if pass0_path.exists() and not record.pass0_root:
                    try:
                        data = json.loads(pass0_path.read_text())
                        record.pass0_root = _root_hex(data.get("root"))
                        if record.pass0_root:
                            record.phase = "pass0_ready"
                    except Exception:
                        pass
                if final_path.exists() and not record.final_timing:
                    try:
                        data = json.loads(final_path.read_text())
                        if isinstance(data, dict):
                            record.final_timing = data
                            record.phase = "final_ready"
                    except Exception:
                        pass
                if final_summary_path.exists() and not record.final_summary:
                    try:
                        data = json.loads(final_summary_path.read_text())
                        if isinstance(data, dict):
                            record.final_summary = data
                            record.phase = "proof_ready"
                    except Exception:
                        pass
            if record.phase == "final_ready" and final_summary_path.exists():
                with record.lock:
                    if not record.final_summary:
                        try:
                            record.final_summary = json.loads(final_summary_path.read_text())
                        except Exception:
                            record.final_summary = {}
                    record.phase = "proof_ready"
                break
            time.sleep(0.02)
        rc = proc.poll()
        with record.lock:
            if record.phase not in {"final_ready", "proof_ready"}:
                if rc not in (0, None):
                    stderr = ""
                    try:
                        _, stderr = proc.communicate(timeout=1)
                    except Exception:
                        pass
                    record.phase = "failed"
                    record.error = f"workload exited rc={rc} stderr_tail={str(stderr)[-500:]}"
                elif final_path.exists():
                    try:
                        record.final_timing = json.loads(final_path.read_text())
                        record.phase = "final_ready"
                    except Exception:
                        record.phase = "failed"
                        record.error = "final timing unreadable"
                if final_summary_path.exists():
                    try:
                        record.final_summary = json.loads(final_summary_path.read_text())
                        record.phase = "proof_ready"
                    except Exception:
                        pass

    def start_job(self, payload: dict[str, Any]) -> str:
        job_id = str(payload.get("job_id") or uuid.uuid4())
        lease_id = str(payload.get("lease_id") or "").strip()
        if not lease_id:
            raise ValueError("lease_id is required")
        out_dir = Path(tempfile.mkdtemp(prefix="verathos_audit_worker_"))
        challenge_file = out_dir / f"{lease_id}_challenge.txt"
        script = self._workspace_script()
        record = AuditJobRecord(
            job_id=job_id,
            lease_id=lease_id,
            audit_id=str(payload.get("audit_id") or ""),
            out_dir=out_dir,
            challenge_file=challenge_file,
        )
        cmd = self._build_cmd(
            script=script,
            record=record,
            payload=payload,
            proof_seed=str(payload.get("proof_seed_hex") or ""),
        )
        env = self._workspace_build_env(script.parent)
        try:
            record.proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except Exception as exc:
            raise RuntimeError(f"failed to start audit workload: {exc}") from exc
        record.monitor_thread = threading.Thread(
            target=self._monitor_job,
            args=(record,),
            name=f"audit-job-{job_id[:8]}",
            daemon=True,
        )
        record.monitor_thread.start()
        with self._lock:
            self._jobs[job_id] = record
        return job_id

    def get_job(self, job_id: str) -> Optional[AuditJobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def submit_proof_challenge(self, job_id: str, challenge_seed: str) -> None:
        record = self.get_job(job_id)
        if record is None:
            raise KeyError(job_id)
        seed = str(challenge_seed or "").strip()
        if not seed:
            raise ValueError("challenge_seed required")
        tmp = record.challenge_file.with_suffix(record.challenge_file.suffix + ".tmp")
        tmp.write_text(seed + "\n")
        os.replace(tmp, record.challenge_file)

    def cancel_job(self, job_id: str) -> None:
        record = self.get_job(job_id)
        if record is None:
            return
        proc = record.proc
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
        with record.lock:
            record.phase = "cancelled"

    def active_job_count(self) -> int:
        with self._lock:
            count = 0
            for record in self._jobs.values():
                proc = record.proc
                if proc is not None and proc.poll() is None:
                    count += 1
            return count
