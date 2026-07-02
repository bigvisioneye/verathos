"""HTTP client for audit worker balancer (Balancer 2)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

import httpx


@dataclass(frozen=True)
class AuditWorkerLease:
    lease_id: str
    worker_id: str
    endpoint: str
    worker_key: str
    gpu_class: str
    lease_ttl_s: float


class CapacityAuditBalancerClient:
    def __init__(
        self,
        base_url: str,
        *,
        api_key: str = "",
        timeout_s: float = 5.0,
    ):
        self.base_url = str(base_url or "").rstrip("/")
        self.api_key = str(api_key or os.environ.get("CAPACITY_AUDIT_BALANCER_API_KEY", "") or "").strip()
        self.timeout_s = max(1.0, float(timeout_s))

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def pick_worker(self, gpu_class: str) -> Optional[AuditWorkerLease]:
        if not self.base_url:
            return None
        query = urlencode({"gpu_class": str(gpu_class or "").strip()})
        url = f"{self.base_url}/pick1?{query}"
        resp = httpx.get(url, headers=self._headers(), timeout=self.timeout_s)
        resp.raise_for_status()
        data = resp.json() or {}
        endpoint = str(data.get("endpoint") or "").rstrip("/")
        worker_key = str(data.get("worker_key") or "").strip()
        lease_id = str(data.get("lease_id") or "").strip()
        if not endpoint or not worker_key or not lease_id:
            raise RuntimeError(f"audit balancer /pick1 missing fields: {data}")
        return AuditWorkerLease(
            lease_id=lease_id,
            worker_id=str(data.get("worker_id") or ""),
            endpoint=endpoint,
            worker_key=worker_key,
            gpu_class=str(data.get("gpu_class") or gpu_class),
            lease_ttl_s=float(data.get("lease_ttl_s") or 0.0),
        )

    def release(self, lease_id: str) -> None:
        if not self.base_url or not lease_id:
            return
        url = f"{self.base_url}/v1/release"
        httpx.post(
            url,
            headers=self._headers(),
            json={"lease_id": lease_id},
            timeout=self.timeout_s,
        )

    def register_worker(self, payload: dict[str, Any]) -> None:
        if not self.base_url:
            return
        url = f"{self.base_url}/v1/workers/register"
        httpx.post(url, headers=self._headers(), json=payload, timeout=self.timeout_s)

    def heartbeat_worker(self, payload: dict[str, Any]) -> None:
        if not self.base_url:
            return
        url = f"{self.base_url}/v1/workers/heartbeat"
        httpx.post(url, headers=self._headers(), json=payload, timeout=self.timeout_s)
