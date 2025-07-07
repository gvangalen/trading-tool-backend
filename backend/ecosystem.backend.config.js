module.exports = {
  apps: [
    {
      name: 'backend',
      script: 'start_backend.py',              // ⬅️ Geen 'backend/' prefix meer
      interpreter: '/usr/bin/python3',
      cwd: '/home/ubuntu/trading-tool-backend/backend', // ⬅️ Ga direct naar de /backend map
      watch: false,
      env: {
        ENV: 'production',
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '.',                        // ✅ Hierdoor werken imports als api.setups_api
      },
    },
    {
      name: 'celery',
      script: 'celery',
      args: '-A celery_task.celery_app worker --loglevel=info', // ⬅️ Geen 'backend.' prefix
      cwd: '/home/ubuntu/trading-tool-backend/backend',
      interpreter: '/bin/bash',
      env: {
        CELERY_BROKER_URL: 'redis://localhost:6379/0',
        CELERY_RESULT_BACKEND: 'redis://localhost:6379/0',
        PYTHONPATH: '.',                        // ✅ Ook nodig voor celery
      },
    },
  ],
};
