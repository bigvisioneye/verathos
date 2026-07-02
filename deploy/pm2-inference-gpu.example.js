// Example PM2 config: inference GPU (side 2) — vLLM + proofs, accepts proxy-forwarded traffic.
//
//   pm2 start deploy/pm2-inference-gpu.example.js --only inference-qwen9b

module.exports = {
  apps: [
    {
      name: "inference-qwen9b",
      script: ".venv-vllm/bin/python",
      args: [
        "-u", "-m", "verallm.api.server",
        "--model-id", "Qwen/Qwen3.5-9B",
        "--quant", "fp16",
        "--port", "8000",
        "--host", "0.0.0.0",
      ].join(" "),
      cwd: "/workspace/verathos",
      env: {
        VERATHOS_PROXY_LLM_KEY: "REPLACE_PROXY_LLM_KEY",
      },
      autorestart: true,
      merge_logs: true,
    },
  ],
};
