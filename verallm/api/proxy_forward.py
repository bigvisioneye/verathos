"""Proxy inference forwarding via Balancer 1."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncIterator, Optional
from urllib.parse import urlencode

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from verallm.api.proxy_auth import PROXY_LLM_HEADER

logger = logging.getLogger(__name__)


class ProxyInferenceState:
    def __init__(self) -> None:
        self.enabled: bool = False
        self.balancer_base: str = ""
        self.balancer_api_key: str = ""
        self.proxy_llm_key: str = ""
        self.model_id: str = ""
        self.quant: str = ""
        self.max_context_len: int = 0
        self.slot_id: str = ""
        self.verify_upstream_ssl: bool = True


proxy_state = ProxyInferenceState()


def configure_proxy_from_args(args) -> None:
    balancer = str(getattr(args, "proxy_balancer", "") or os.environ.get("PROXY_BALANCER_URL", "") or "").strip()
    if not balancer and not getattr(args, "proxy_mode", False):
        return
    proxy_state.enabled = True
    proxy_state.balancer_base = balancer.rstrip("/")
    proxy_state.balancer_api_key = str(
        getattr(args, "proxy_balancer_key", "")
        or os.environ.get("PROXY_BALANCER_API_KEY", "")
        or ""
    ).strip()
    proxy_state.proxy_llm_key = str(
        getattr(args, "proxy_llm_key", "")
        or os.environ.get("PROXY_LLM_KEY", "")
        or ""
    ).strip()
    proxy_state.model_id = str(
        getattr(args, "model_id", "")
        or getattr(args, "model", "")
        or ""
    )
    proxy_state.quant = str(getattr(args, "quant", "") or "auto")
    proxy_state.max_context_len = int(getattr(args, "max_model_len", 0) or 0)
    proxy_state.slot_id = str(
        getattr(args, "proxy_slot_id", "")
        or os.environ.get("PROXY_SLOT_ID", "")
        or ""
    ).strip()
    proxy_state.verify_upstream_ssl = str(
        os.environ.get("PROXY_UPSTREAM_VERIFY_SSL", "1")
    ).strip().lower() not in {"0", "false", "no"}


def apply_advertised_hardware(state, args) -> None:
    """Populate /health hardware from CLI/env on proxy nodes without CUDA."""
    gpu_name = str(getattr(args, "advertised_gpu_name", "") or os.environ.get("VERATHOS_ADVERTISED_GPU_NAME", "") or "")
    vram_gb = getattr(args, "advertised_vram_gb", None)
    if vram_gb is None:
        raw = os.environ.get("VERATHOS_ADVERTISED_VRAM_GB", "")
        vram_gb = int(raw) if str(raw).strip().isdigit() else 0
    gpu_count = getattr(args, "advertised_gpu_count", None)
    if gpu_count is None:
        raw = os.environ.get("VERATHOS_ADVERTISED_GPU_COUNT", "1")
        gpu_count = int(raw) if str(raw).strip().isdigit() else 1
    compute_capability = str(
        getattr(args, "advertised_compute_capability", "")
        or os.environ.get("VERATHOS_ADVERTISED_COMPUTE_CAPABILITY", "")
        or ""
    )
    uuids_raw = str(getattr(args, "advertised_gpu_uuids", "") or os.environ.get("VERATHOS_ADVERTISED_GPU_UUIDS", "") or "")
    uuids = [u.strip() for u in uuids_raw.split(",") if u.strip()]
    if gpu_name:
        state.gpu_name = gpu_name
    if vram_gb:
        state.vram_gb = int(vram_gb)
    if gpu_count:
        state.gpu_count = int(gpu_count)
    if compute_capability:
        state.compute_capability = compute_capability
    if uuids:
        state.gpu_uuids = uuids


def _balancer_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if proxy_state.balancer_api_key:
        headers["Authorization"] = f"Bearer {proxy_state.balancer_api_key}"
    return headers


def _validator_hotkey(request: Optional[Request]) -> str:
    if request is None:
        return ""
    from_request = str(
        getattr(getattr(request, "state", None), "validator_hotkey", "") or ""
    ).strip()
    if from_request:
        return from_request
    return str(request.headers.get("X-Validator-Hotkey", "") or "").strip()


def _pick_upstream() -> dict[str, Any]:
    if not proxy_state.balancer_base:
        raise RuntimeError("proxy balancer URL not configured")
    params = {
        "model_id": proxy_state.model_id,
        "quant": proxy_state.quant,
    }
    if proxy_state.max_context_len > 0:
        params["max_context_len"] = str(proxy_state.max_context_len)
    if proxy_state.slot_id:
        params["slot_id"] = proxy_state.slot_id
    url = f"{proxy_state.balancer_base}/pick?{urlencode(params)}"
    resp = httpx.get(url, headers=_balancer_headers(), timeout=5.0)
    resp.raise_for_status()
    data = resp.json() or {}
    endpoint = str(data.get("endpoint") or "").rstrip("/")
    if not endpoint:
        raise RuntimeError(f"balancer /pick missing endpoint: {data}")
    logger.info(
        "proxy balancer pick ok: upstream=%s worker_id=%s",
        endpoint,
        str(data.get("worker_id") or data.get("slot_id") or ""),
    )
    return data


def _upstream_headers(pick: dict[str, Any], request: Optional[Request]) -> dict[str, str]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    api_key = str(pick.get("api_key") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if proxy_state.proxy_llm_key:
        headers[PROXY_LLM_HEADER] = proxy_state.proxy_llm_key
    if request is not None:
        for name in (
            "X-Request-Id",
            "X-Validator-Hotkey",
            "X-Validator-Signature",
            "X-Validator-Timestamp",
        ):
            value = request.headers.get(name)
            if value:
                headers[name] = value
    return headers


async def _stream_upstream_response(resp: httpx.Response) -> AsyncIterator[bytes]:
    async for chunk in resp.aiter_bytes():
        yield chunk


async def proxy_json_post(
    path: str,
    body: dict[str, Any],
    request: Optional[Request] = None,
) -> Any:
    pick = _pick_upstream()
    endpoint = str(pick["endpoint"]).rstrip("/")
    url = f"{endpoint}{path}"
    validator_hotkey = _validator_hotkey(request)
    hotkey_suffix = f" validator={validator_hotkey[:12]}..." if validator_hotkey else ""
    logger.info("proxy forwarding %s -> %s%s", path, url, hotkey_suffix)
    client = httpx.AsyncClient(verify=proxy_state.verify_upstream_ssl, timeout=None)
    req = client.build_request(
        "POST",
        url,
        headers=_upstream_headers(pick, request),
        json=body,
    )
    resp = await client.send(req, stream=True)
    logger.info(
        "proxy upstream response: path=%s status=%s upstream=%s%s",
        path,
        resp.status_code,
        endpoint,
        hotkey_suffix,
    )
    if resp.status_code >= 400:
        try:
            raw = await resp.aread()
            try:
                content = json.loads(raw.decode()) if raw else {"error": resp.reason_phrase}
            except json.JSONDecodeError:
                content = {"error": raw.decode(errors="replace")}
            return JSONResponse(status_code=resp.status_code, content=content)
        finally:
            await resp.aclose()
            await client.aclose()

    content_type = resp.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        async def sse_iter():
            try:
                async for chunk in resp.aiter_bytes():
                    yield chunk
            finally:
                await resp.aclose()
                await client.aclose()

        return StreamingResponse(
            sse_iter(),
            status_code=resp.status_code,
            media_type=content_type,
            headers={
                k: v
                for k, v in resp.headers.items()
                if k.lower() in {"cache-control", "x-accel-buffering"}
            },
        )

    try:
        raw = await resp.aread()
        if raw:
            try:
                return json.loads(raw.decode())
            except json.JSONDecodeError:
                return JSONResponse(status_code=resp.status_code, content={"raw": raw.decode(errors="replace")})
        return JSONResponse(status_code=resp.status_code, content={})
    finally:
        await resp.aclose()
        await client.aclose()


def proxy_startup_minimal(state, args) -> None:
    """Initialize proxy-only server state without loading vLLM."""
    configure_proxy_from_args(args)
    apply_advertised_hardware(state, args)
    state.model_name = str(getattr(args, "model_id", "") or getattr(args, "model", "") or "")
    if getattr(args, "evm_address", None):
        state.evm_address = args.evm_address
    if getattr(args, "evm_private_key", None):
        state.evm_private_key = args.evm_private_key
    state.capacity_audit_state_file = str(getattr(args, "capacity_audit_state_file", "") or "")
