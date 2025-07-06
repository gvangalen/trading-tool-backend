module.exports = {
  apps: [
    {
      name: 'celery',
      cwd: '/home/ubuntu/trading-tool-backend',
      script: 'venv/bin/celery',
      args: '-A celery_app worker --loglevel=info',
      interpreter: 'none',
      env: {
        CELERY_BROKER_URL: 'redis://localhost:6379/0',
        CELERY_RESULT_BACKEND: 'redis://localhost:6379/0',
      },
    },
  ],
};
