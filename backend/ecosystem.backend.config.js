module.exports = {
  apps: [
    {
      name: 'backend',
      script: 'backend/start_backend.py', // ⬅️ Dit is je echte script
      interpreter: '/usr/bin/python3',    // ⬅️ Gebruik expliciete Python path
      cwd: '/home/ubuntu/trading-tool-backend',
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
