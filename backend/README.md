# 📈 Market Dashboard - Backend

Dit is de backend API en services voor het Market Dashboard project.  
De backend levert data aan de frontend en verwerkt automatische analyses via Celery workers.

---

## 🚀 Features

- FastAPI server voor API endpoints
- Celery workers voor achtergrondtaken
- SQLite database (`market_data.db`)
- Periodieke updates van markt-, macro- en technische data
- AI-analyse voor trading strategieën en rapporten
- Docker support voor eenvoudige deployment

---

## 📦 Installatie

### 1. Project clonen
```bash
git clone https://github.com/jouwgebruikersnaam/backend-repository.git
cd backend-repository

2. Virtuele omgeving aanmaken (optioneel maar aanbevolen)

python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

3. Vereisten installeren
pip install -r requirements.txt

Belangrijke packages:
	•	fastapi — API server
	•	uvicorn[standard] — Server voor FastAPI
	•	sqlalchemy — ORM voor SQLite database
	•	celery + redis — Achtergrondtaken
	•	python-dotenv — Laden van .env configuraties
	•	openai, httpx, pandas, requests — Data-analyse en AI-taken

Maak een .env bestand aan in de root van je project met bijvoorbeeld:
OPENAI_API_KEY=your-openai-key
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=sqlite:///market_data.db

🖥️ Starten van de applicatie

1. FastAPI server starten
uvicorn app.main:app --reload
De server draait dan op: http://localhost:8000

2. Celery worker starten
celery -A app.celery_worker.celery_app worker --loglevel=info
Zorg dat Redis draait voordat je Celery start.

📊 Belangrijke API Endpoints

Beschrijving
/api/market_data
Haalt actuele marktdata op
/api/macro_data
Haalt macro-economische data op
/api/technical_data
Haalt technische indicatoren op
/api/setups
Setupmanagement
/api/strategies
Strategiebeheer (AI of handmatig)
/api/daily_report
Genereert dagelijks rapport

🐳 Docker (optioneel)

Wil je alles in Docker draaien?
	1.	Zorg dat Docker en Docker Compose geïnstalleerd zijn.
	2.	Maak een docker-compose.yml bestand.
	3.	Start containers:
docker-compose up

📚 TODO / Roadmap
	•	OAuth2 beveiliging
	•	Volledige Docker productie-deployment
	•	Meer AI-taken automatiseren
	•	Notificaties (e-mail, Telegram)

✅ Klaar om te starten!

Veel succes en happy trading! 🚀

