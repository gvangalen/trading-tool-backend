module.exports = {
  apps: [
    {
      name: 'backend',
      script: 'python3',
      args: 'app.py',
      cwd: '/home/ubuntu/trading-tool-backend',
      interpreter: 'python3',
      watch: false,
      env: {
        ENV: 'production',
      },
    },
    {
      name: 'celery',
      script: 'celery',
      args: '-A backend.celery_task.celery_app worker --loglevel=info',
      cwd: '/home/ubuntu/trading-tool-backend',
      interpreter: '/bin/bash',
      env: {
        CELERY_BROKER_URL: 'redis://localhost:6379/0',
        CELERY_RESULT_BACKEND: 'redis://localhost:6379/0',
      },
    },
  ],
};
