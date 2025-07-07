module.exports = {
  apps: [
    {
      name: 'backend',
      script: 'start_backend.py', // ⬅️ draait in backend-map
      interpreter: '/usr/bin/python3',
      cwd: '/home/ubuntu/trading-tool-backend/backend',
      watch: false,
      env: {
        ENV: 'production',

        // ✅ PostgreSQL
        DB_HOST: '127.0.0.1',
        DB_PORT: '5432',
        DB_NAME: 'market_dashboard',
        DB_USER: 'dashboard_user',
        DB_PASSWORD: 'GVG_88_database',

        // ✅ API base URL
        API_BASE_URL: 'http://127.0.0.1:5002/api',

        // ✅ Redis (voor Celery tasks vanuit backend)
        CELERY_BROKER_URL: 'redis://localhost:6379/0',
        CELERY_RESULT_BACKEND: 'redis://localhost:6379/0',

        // ✅ OpenAI + AI modus
        OPENAI_API_KEY: 'your-real-openai-key',
        AI_MODE: 'live'
      }
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
