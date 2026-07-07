"""HTTP client for remote capacity-audit worker jobs (side 3)."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from neurons.capacity_audit_combined import proof_summary_ready
from verallm.api.proxy_auth import AUDIT_WORKER_HEADER


def audit_worker_verify_ssl() -> bool:
    return str(os.environ.get("PROXY_UPSTREAM_VERIFY_SSL", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


@dataclass
class RemoteAuditJobStatus:
    job_id: str
    phase: str
    pass0_root: str = ""
    final_timing: dict = None  # type: ignore[assignment]
    final_summary: dict = None  # type: ignore[assignment]
    error: str = ""

    def __post_init__(self) -> None:
        if self.final_timing is None:
            self.final_timing = {}
        if self.final_summary is None:
            self.final_summary = {}


class RemoteAuditClient:
    def __init__(
        self,
        endpoint: str,
        worker_key: str,
        *,
        timeout_s: float = 30.0,
    ):
        self.endpoint = str(endpoint or "").rstrip("/")
        self.worker_key = str(worker_key or "").strip()
        self.timeout_s = max(1.0, float(timeout_s))
        self.verify_ssl = audit_worker_verify_ssl()

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            AUDIT_WORKER_HEADER: self.worker_key,
        }

    def start_job(self, payload: dict[str, Any]) -> str:
        url = f"{self.endpoint}/capacity-audit/v1/jobs"
        resp = httpx.post(
            url,
            headers=self._headers(),
            json=payload,
            timeout=self.timeout_s,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        data = resp.json() or {}
        job_id = str(data.get("job_id") or "").strip()
        if not job_id:
            raise RuntimeError(f"audit worker missing job_id: {data}")
        return job_id

    def get_job(self, job_id: str) -> RemoteAuditJobStatus:
        url = f"{self.endpoint}/capacity-audit/v1/jobs/{job_id}"
        resp = httpx.get(url, headers=self._headers(), timeout=self.timeout_s, verify=self.verify_ssl)
        resp.raise_for_status()
        data = resp.json() or {}
        pass0 = data.get("pass0") if isinstance(data.get("pass0"), dict) else {}
        return RemoteAuditJobStatus(
            job_id=str(data.get("job_id") or job_id),
            phase=str(data.get("phase") or ""),
            pass0_root=str(pass0.get("root") or data.get("pass0_root") or ""),
            final_timing=data.get("final_timing") if isinstance(data.get("final_timing"), dict) else {},
            final_summary=data.get("final_summary") if isinstance(data.get("final_summary"), dict) else {},
            error=str(data.get("error") or ""),
        )

    def submit_proof_challenge(self, job_id: str, challenge_seed: str) -> None:
        url = f"{self.endpoint}/capacity-audit/v1/jobs/{job_id}/proof-challenge"
        resp = httpx.post(
            url,
            headers=self._headers(),
            json={"challenge_seed": challenge_seed},
            timeout=self.timeout_s,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()

    def cancel_job(self, job_id: str) -> None:
        url = f"{self.endpoint}/capacity-audit/v1/jobs/{job_id}"
        try:
            httpx.delete(url, headers=self._headers(), timeout=self.timeout_s, verify=self.verify_ssl)
        except Exception:
            pass

    def wait_for_phase(
        self,
        job_id: str,
        phase: str,
        *,
        timeout_s: float,
        poll_s: float = 0.05,
    ) -> RemoteAuditJobStatus:
        deadline = time.time() + max(1.0, float(timeout_s))
        target = str(phase or "").strip()
        while time.time() < deadline:
            status = self.get_job(job_id)
            if status.phase == "failed":
                raise RuntimeError(status.error or "remote audit job failed")
            if status.phase == target:
                return status
            if target == "final_ready" and status.phase == "proof_ready":
                return status
            time.sleep(max(0.02, float(poll_s)))
        raise TimeoutError(f"remote audit job {job_id} timed out waiting for {target}")

    def wait_for_proof_summary(
        self,
        job_id: str,
        *,
        timeout_s: float,
        poll_s: float = 0.05,
    ) -> RemoteAuditJobStatus:
        deadline = time.time() + max(1.0, float(timeout_s))
        premature_phase = False
        while time.time() < deadline:
            status = self.get_job(job_id)
            if status.phase == "failed":
                raise RuntimeError(status.error or "remote audit job failed")
            if proof_summary_ready(status.final_summary):
                return status
            if status.phase == "proof_ready" and not premature_phase:
                premature_phase = True
            time.sleep(max(0.02, float(poll_s)))
        status = self.get_job(job_id)
        detail = (
            f"phase={status.phase} premature_proof_ready={premature_phase} "
            f"summary_keys={sorted(status.final_summary.keys()) if status.final_summary else []}"
        )
        raise TimeoutError(
            f"remote audit job {job_id} timed out waiting for proof summary ({detail})"
        )
