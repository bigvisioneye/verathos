// Example PM2 config: audit worker GPU (side 3).
//
//   pm2 start deploy/pm2-audit-worker.example.js --only audit-worker-a100-0

module.exports = {
  apps: [
    {
      name: "audit-worker-a100-0",
      script: ".venv-vllm/bin/python",
      args: [
        "-u", "-m", "services.audit_worker",
        "--host", "0.0.0.0",
        "--port", "8095",
        "--worker-id", "audit-a100-0",
        "--public-endpoint", "http://10.0.0.30:8095",
        "--gpu-class", "NVIDIA A100-SXM4-80GB",
        "--worker-key", "REPLACE_AUDIT_WORKER_KEY",
      ].join(" "),
      cwd: "/workspace/verathos",
      env: {
        CAPACITY_AUDIT_BALANCER_URL: "http://10.0.0.10:8081",
        CAPACITY_AUDIT_BALANCER_API_KEY: "REPLACE_BALANCER2_KEY",
        VERATHOS_AUDIT_WORKER_KEY: "REPLACE_AUDIT_WORKER_KEY",
      },
      autorestart: true,
      merge_logs: true,
    },
  ],
};
