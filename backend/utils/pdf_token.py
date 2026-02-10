import jwt
from datetime import datetime, timedelta
import os

SECRET = os.getenv("PDF_SECRET", "super-secret-change-this")


def create_pdf_token(user_id: int, report_type: str, date: str):
    payload = {
        "uid": user_id,
        "t": report_type,
        "d": date,
        "exp": datetime.utcnow() + timedelta(minutes=5),
    }

    return jwt.encode(payload, SECRET, algorithm="HS256")


def verify_pdf_token(token: str):
    return jwt.decode(token, SECRET, algorithms=["HS256"])
