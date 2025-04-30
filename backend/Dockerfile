# ✅ Gebruik een lichte base image
FROM python:3.9-slim

# ✅ Stel werkdirectory in
WORKDIR /app

# ✅ Benodigde packages installeren
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    awscli \
 && rm -rf /var/lib/apt/lists/*

# ✅ Kopieer requirements en installeer afhankelijkheden
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Kopieer projectbestanden
COPY . .

# ✅ Standaardpoort voor FastAPI (kan overschreven worden per service)
EXPOSE 5002

# ✅ Standaard commando voor FastAPI-app (overschrijfbaar via docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5002"]

