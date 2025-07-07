module.exports = {
  apps: [
    {
      name: 'backend',
      script: 'start_backend.py',               // ⬅️ draait in backend-map
      interpreter: '/usr/bin/python3',
      cwd: '/home/ubuntu/trading-tool-backend/backend',
      watch: false,
      env: {
        ENV: 'production',
      },
    },
    {
      name: 'celery',
      script: 'celery',
      args: '-A backend.celery_task.celery_app worker --loglevel=info',
      interpreter: '/bin/bash',
      cwd: '/home/ubuntu/trading-tool-backend',
      env: {
        CELERY_BROKER_URL: 'redis://localhost:6379/0',
        CELERY_RESULT_BACKEND: 'redis://localhost:6379/0',
      },
    },
  ],
};
