FROM python:3.9-slim

# ✅ Werkdirectory op rootniveau
WORKDIR /app

# ✅ Installeer systeempakketten
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    awscli \
 && rm -rf /var/lib/apt/lists/*

# ✅ Kopieer alleen requirements en installeer Python dependencies
COPY backend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ✅ Kopieer volledige backend map
COPY backend /app/backend

# ✅ Zet juiste werkmap en PYTHONPATH
ENV PYTHONPATH=/app/backend
WORKDIR /app/backend

# ✅ Start FastAPI-app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5002"]
