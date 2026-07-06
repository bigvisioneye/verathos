#!/usr/bin/env python3
"""Manual smoke test: Balancer2 pick -> audit worker job (same path as proxy miner)."""

from __future__ import annotations

import os
import sys
import time
import uuid

# Repo root on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neurons.capacity_audit_balancer import CapacityAuditBalancerClient
from neurons.capacity_audit_remote import RemoteAuditClient

BALANCER = os.environ.get("CAPACITY_AUDIT_BALANCER_URL", "http://65.108.102.231:8081/api")
BALANCER_KEY = os.environ.get("CAPACITY_AUDIT_BALANCER_API_KEY", "")
WORKER_KEY = os.environ.get("VERATHOS_AUDIT_WORKER_KEY", "")
GPU_CLASS = os.environ.get("VERATHOS_AUDIT_GPU_CLASS", "NVIDIA A100-SXM4-40GB")

# Minimal workload spec for A100-40GB (from subnet capacity table defaults)
MINIMAL_WORKLOAD = {
    "workload_version": "hot_capacity_combined",
    "capacity_matrix_dim": 8960,
    "capacity_passes": 2,
    "capacity_rounds": 1,
    "capacity_warmup_passes": 0,
    "capacity_block_size": 64,
    "transition_mix_rounds": 0,
    "transition_fanout": 1,
    "capacity_tail_passes": 0,
    "capacity_tail_rounds": 1,
    "capacity_tail_warmup_passes": 0,
    "capacity_tail_transition_mix_rounds": 1,
    "capacity_tail_transition_fanout": 1,
    "fp64_matrix_dim": 4096,
    "fp64_passes": 1,
    "fp64_rounds": 1,
    "fp64_warmup_passes": 0,
    "fp64_block_size": 64,
    "spot_checks": 1,
    "pass_count": 3,
}


def main() -> int:
    if not BALANCER_KEY:
        print("ERROR: set CAPACITY_AUDIT_BALANCER_API_KEY")
        return 1
    if not WORKER_KEY:
        print("ERROR: set VERATHOS_AUDIT_WORKER_KEY")
        return 1

    audit_id = f"manual-test-{uuid.uuid4().hex[:12]}"
    lease_id = f"manual-lease-{uuid.uuid4().hex[:12]}"
    print(f"=== 1. pick worker gpu_class={GPU_CLASS} ===")
    balancer = CapacityAuditBalancerClient(BALANCER, api_key=BALANCER_KEY)
    try:
        worker = balancer.pick_worker(GPU_CLASS)
    except Exception as exc:
        print(f"FAIL pick: {exc}")
        return 1
    print(f"OK pick: worker={worker.worker_id} endpoint={worker.endpoint} lease={worker.lease_id}")
    print(f"    worker_key source: {'pick' if worker.worker_key else 'missing'}")

    remote = RemoteAuditClient(worker.endpoint, worker.worker_key, timeout_s=60.0)
    job_id = ""
    try:
        print("=== 2. worker /health ===")
        import httpx
        from verallm.api.proxy_auth import AUDIT_WORKER_HEADER

        try:
            h = httpx.get(
                f"{worker.endpoint}/health",
                headers={AUDIT_WORKER_HEADER: worker.worker_key},
                timeout=15.0,
                verify=False,
            )
            print(f"health: HTTP {h.status_code} {h.text[:300]}")
        except Exception as exc:
            print(f"health: FAIL {exc}")

        print("=== 3. start job ===")
        job_id = remote.start_job(
            {
                "lease_id": lease_id,
                "audit_id": audit_id,
                "proof_seed_hex": "00" * 32,
                "challenge_timeout_s": 120.0,
                "workload_spec": MINIMAL_WORKLOAD,
                "pass_count": MINIMAL_WORKLOAD["pass_count"],
                "gpu_class": GPU_CLASS,
            }
        )
        print(f"OK start: job_id={job_id}")

        print("=== 4. poll job (max 180s) ===")
        deadline = time.time() + 180.0
        last_phase = ""
        while time.time() < deadline:
            status = remote.get_job(job_id)
            if status.phase != last_phase:
                print(f"  phase={status.phase} pass0={bool(status.pass0_root)} "
                      f"final={bool(status.final_timing)} summary={bool(status.final_summary)}")
                last_phase = status.phase
            if status.phase in {"final_ready", "proof_ready"}:
                print("OK workload completed")
                break
            if status.phase == "failed":
                print(f"FAIL job: {status.error}")
                return 1
            if status.phase == "cancelled":
                print("FAIL job cancelled")
                return 1
            time.sleep(2.0)
        else:
            print("TIMEOUT waiting for final_ready/proof_ready")
            return 1
    except Exception as exc:
        print(f"FAIL remote: {exc}")
        return 1
    finally:
        if job_id:
            print("=== 5. cancel job ===")
            remote.cancel_job(job_id)
        print("=== 6. release lease ===")
        try:
            balancer.release(worker.lease_id)
            print(f"OK released lease {worker.lease_id[:8]}...")
        except Exception as exc:
            print(f"WARN release failed: {exc}")

    print("=== PASS ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
