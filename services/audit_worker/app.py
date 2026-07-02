"""FastAPI service for remote capacity-audit workloads (side 3)."""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from typing import Any, Optional

import httpx
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from neurons.capacity_audit_balancer import CapacityAuditBalancerClient
from services.audit_worker.runner import AuditJobRunner
from verallm.api.proxy_auth import AUDIT_WORKER_HEADER, verify_audit_worker_request

app = FastAPI(title="Verathos Audit Worker", version="0.1.0")
runner = AuditJobRunner()

_worker_id = ""
_worker_endpoint = ""
_worker_key = ""
_gpu_class = ""
_balancer: Optional[CapacityAuditBalancerClient] = None
_heartbeat_stop = threading.Event()
_heartbeat_thread: Optional[threading.Thread] = None


class StartJobBody(BaseModel):
    lease_id: str
    audit_id: str = ""
    proof_seed_hex: str = ""
    challenge_timeout_s: float = 120.0
    workload_spec: dict[str, Any] = Field(default_factory=dict)
    pass_count: int = 0
    gpu_class: str = ""
    job_id: str = ""


class ProofChallengeBody(BaseModel):
    challenge_seed: str


def _detect_gpu_class() -> str:
    override = str(os.environ.get("VERATHOS_AUDIT_GPU_CLASS", "") or "").strip()
    if override:
        return override
    try:
        import torch

        if torch.cuda.is_available():
            return str(torch.cuda.get_device_properties(0).name)
    except Exception:
        pass
    return "unknown"


def _detect_hardware() -> dict[str, Any]:
    try:
        import torch

        if not torch.cuda.is_available():
            return {}
        props = torch.cuda.get_device_properties(0)
        cc = torch.cuda.get_device_capability(0)
        from verallm.registry.gpu import detect_vram_gb

        vram_gb = detect_vram_gb()
        uuids = []
        for i in range(torch.cuda.device_count()):
            try:
                uuids.append(str(torch.cuda.get_device_properties(i).uuid))
            except Exception:
                pass
        return {
            "gpu_name": props.name,
            "gpu_count": torch.cuda.device_count(),
            "vram_gb": int(vram_gb),
            "compute_capability": f"{cc[0]}.{cc[1]}",
            "gpu_uuids": uuids,
        }
    except Exception:
        return {}


def _require_worker_key(request: Request) -> None:
    if not verify_audit_worker_request(request, _worker_key):
        raise HTTPException(status_code=401, detail="invalid audit worker key")


@app.get("/health")
async def health():
    hw = _detect_hardware()
    return {
        "status": "ok",
        "service": "audit_worker",
        "worker_id": _worker_id,
        "gpu_class": _gpu_class or _detect_gpu_class(),
        "active_jobs": runner.active_job_count(),
        "hardware": hw or None,
    }


@app.post("/capacity-audit/v1/jobs")
async def start_job(body: StartJobBody, request: Request):
    _require_worker_key(request)
    if runner.active_job_count() >= 1:
        raise HTTPException(status_code=503, detail="audit worker busy")
    payload = body.model_dump()
    try:
        job_id = runner.start_job(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"job_id": job_id, "phase": "starting"}


@app.get("/capacity-audit/v1/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    _require_worker_key(request)
    record = runner.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    with record.lock:
        return {
            "job_id": record.job_id,
            "audit_id": record.audit_id,
            "lease_id": record.lease_id,
            "phase": record.phase,
            "pass0": {"root": record.pass0_root} if record.pass0_root else {},
            "pass0_root": record.pass0_root,
            "final_timing": record.final_timing,
            "final_summary": record.final_summary,
            "error": record.error,
        }


@app.post("/capacity-audit/v1/jobs/{job_id}/proof-challenge")
async def proof_challenge(job_id: str, body: ProofChallengeBody, request: Request):
    _require_worker_key(request)
    try:
        runner.submit_proof_challenge(job_id, body.challenge_seed)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="job not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@app.delete("/capacity-audit/v1/jobs/{job_id}")
async def cancel_job(job_id: str, request: Request):
    _require_worker_key(request)
    runner.cancel_job(job_id)
    return {"ok": True}


def _heartbeat_loop() -> None:
    if _balancer is None or not _worker_id:
        return
    while not _heartbeat_stop.wait(15.0):
        try:
            _balancer.heartbeat_worker(
                {
                    "worker_id": _worker_id,
                    "endpoint": _worker_endpoint,
                    "worker_key": _worker_key,
                    "gpu_class": _gpu_class,
                    "active_jobs": runner.active_job_count(),
                }
            )
        except Exception:
            pass


def _register_with_balancer() -> None:
    global _balancer
    balancer_url = str(os.environ.get("CAPACITY_AUDIT_BALANCER_URL", "") or "").strip()
    if not balancer_url:
        return
    api_key = str(os.environ.get("CAPACITY_AUDIT_BALANCER_API_KEY", "") or "").strip()
    _balancer = CapacityAuditBalancerClient(balancer_url, api_key=api_key)
    payload = {
        "worker_id": _worker_id,
        "endpoint": _worker_endpoint,
        "worker_key": _worker_key,
        "gpu_class": _gpu_class,
        "lease_ttl_s": 60,
    }
    _balancer.register_worker(payload)


def configure_worker(
    *,
    worker_id: str,
    endpoint: str,
    worker_key: str,
    gpu_class: str = "",
) -> None:
    global _worker_id, _worker_endpoint, _worker_key, _gpu_class, _heartbeat_thread
    _worker_id = worker_id
    _worker_endpoint = endpoint.rstrip("/")
    _worker_key = worker_key
    _gpu_class = gpu_class or _detect_gpu_class()
    _register_with_balancer()
    if _balancer is not None:
        _heartbeat_stop.clear()
        _heartbeat_thread = threading.Thread(target=_heartbeat_loop, name="audit-worker-heartbeat", daemon=True)
        _heartbeat_thread.start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verathos capacity-audit worker")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--worker-id", default=None, help="Unique worker id for balancer registration")
    parser.add_argument("--public-endpoint", default=None, help="URL balancers/proxies use to reach this worker")
    parser.add_argument("--worker-key", default=None, help="Shared secret (or VERATHOS_AUDIT_WORKER_KEY)")
    parser.add_argument("--gpu-class", default=None, help="Exact GPU class string for balancer routing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    worker_key = str(args.worker_key or os.environ.get("VERATHOS_AUDIT_WORKER_KEY", "") or "").strip()
    if not worker_key:
        raise SystemExit("audit worker requires --worker-key or VERATHOS_AUDIT_WORKER_KEY")
    worker_id = str(args.worker_id or os.environ.get("VERATHOS_AUDIT_WORKER_ID", "") or "").strip()
    if not worker_id:
        worker_id = f"audit-worker-{args.host}:{args.port}"
    public_endpoint = str(
        args.public_endpoint
        or os.environ.get("VERATHOS_AUDIT_WORKER_ENDPOINT", "")
        or f"http://{args.host}:{args.port}"
    ).strip()
    configure_worker(
        worker_id=worker_id,
        endpoint=public_endpoint,
        worker_key=worker_key,
        gpu_class=str(args.gpu_class or ""),
    )
    uvicorn.run(app, host=args.host, port=args.port, access_log=False, log_level="info")


if __name__ == "__main__":
    main()
