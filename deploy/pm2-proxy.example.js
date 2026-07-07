// Example PM2 config: proxy VPS (side 1) — no GPU, forwards /chat + remote capacity audit.
//
//   pm2 start deploy/pm2-proxy.example.js --only proxy-slot-a
//
// IMPORTANT:
// - /health hardware must match the audit worker GPU class (not the inference GPU).
// - --model-id / --quant must pass the validator capacity model gate for advertised VRAM.
// - Set VERATHOS_ADVERTISED_GPU_NAME via env when the name contains spaces.
// - Capacity audit requires hosted subnet runtime config (windows_per_epoch=5 on mainnet).
//   On startup you should see: "Applied runtime subnet config version=... source=server"
//   If fetch fails from the proxy VPS, seed cache once:
//     mkdir -p ~/.verathos && curl -o ~/.verathos/subnet_config_cache.json https://api.verathos.ai/v1/subnet-config
//   Or pass CLI overrides that match hosted config, e.g. --capacity-audit-windows-per-epoch 5

module.exports = {
  apps: [
    {
      name: "proxy-slot-a",
      script: ".venv-vllm/bin/python",
      args: [
        "-u", "-m", "neurons.miner",
        "--wallet", "verathos",
        "--hotkey", "miner1",
        "--netuid", "96",
        "--subtensor-network", "finney",
        "--model-id", "Qwen/Qwen3.5-9B",
        "--quant", "fp16",
        "--endpoint", "https://proxy.example.com:31123",
        "--capacity-audit",
        "--proxy-mode",
        "--proxy-balancer", "http://10.0.0.10:8080",
        "--proxy-balancer-key", "REPLACE_BALANCER1_KEY",
        "--proxy-llm-key", "REPLACE_PROXY_LLM_KEY",
        "--capacity-audit-balancer", "http://10.0.0.10:8081",
        "--capacity-audit-balancer-key", "REPLACE_BALANCER2_KEY",
        "--advertised-vram-gb", "40",
        "--advertised-gpu-uuids", "REPLACE_A100_GPU_UUID",
        "--",
        "--port", "9062",
        "--proxy-mode",
        "--skip-gpu-check",
      ],
      cwd: "/workspace/verathos",
      env: {
        VERATHOS_ADVERTISED_GPU_NAME: "NVIDIA A100-SXM4-40GB",
        VERATHOS_ADVERTISED_VRAM_GB: "40",
        // Set to 0 if Balancer 1 returns https:// GPU endpoints with self-signed certs.
        PROXY_UPSTREAM_VERIFY_SSL: "0",
      },
      autorestart: true,
      merge_logs: true,
    },
  ],
};
