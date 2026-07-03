// Example PM2 config: proxy VPS (side 1) — no GPU, forwards /chat + remote capacity audit.
//
//   pm2 start deploy/pm2-proxy.example.js --only proxy-slot-a

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
        "--advertised-vram-gb", "80",
        "--advertised-gpu-uuids", "af0b6e27-71dd-4c09-bca2-d7408c800f6a",
        "--",
        "--port", "9062",
        "--proxy-mode",
        "--skip-gpu-check",
      ],
      cwd: "/workspace/verathos",
      env: {
        VERATHOS_ADVERTISED_GPU_NAME: "NVIDIA A100-SXM4-80GB",
        VERATHOS_ADVERTISED_VRAM_GB: "80",
        // Set to 0 if Balancer 1 returns https:// GPU endpoints with self-signed certs.
        // Prefer registering inference GPUs as http:// in Balancer 1 when possible.
        PROXY_UPSTREAM_VERIFY_SSL: "0",
      },
      autorestart: true,
      merge_logs: true,
    },
  ],
};
