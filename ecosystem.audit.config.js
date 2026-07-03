// Side 3: capacity-audit worker GPU
//
//   pm2 start ecosystem.audit.config.js --only audit-worker-a100-0
//
// gpu-class has spaces — set via VERATHOS_AUDIT_GPU_CLASS env, NOT a split-prone CLI string.

module.exports = {
  apps: [
    {
      name: "audit-worker-a100-0",
      script: ".venv-vllm/bin/python",
      cwd: "/workspace/verathos",
      args: [
        "-u", "-m", "services.audit_worker",
        "--host", "0.0.0.0",
        "--port", "8095",
        "--worker-id", "audit-a100-0",
        "--public-endpoint", "http://REPLACE_AUDIT_GPU_IP:8095",
        "--worker-key", "REPLACE_AUDIT_WORKER_KEY",
      ],
      env: {
        CAPACITY_AUDIT_BALANCER_URL: "http://REPLACE_BALANCER2_HOST:8081",
        CAPACITY_AUDIT_BALANCER_API_KEY: "REPLACE_BALANCER2_KEY",
        VERATHOS_AUDIT_WORKER_KEY: "REPLACE_AUDIT_WORKER_KEY",
        VERATHOS_AUDIT_GPU_CLASS: "NVIDIA A100-SXM4-40GB",
        CUDA_VISIBLE_DEVICES: "0",
        PATH: "/workspace/verathos/.venv-vllm/bin:/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      },
      autorestart: true,
      merge_logs: true,
    },
  ],
};
