import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

import jwt
from jwt import PyJWTError
from fastapi import Cookie, HTTPException, status
from passlib.context import CryptContext

# =========================================================
# 🔐 CONFIG
# =========================================================

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_ME_IN_PRODUCTION")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# =========================================================
# 🧂 PASSWORD HELPERS
# =========================================================

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


# =========================================================
# 🎫 JWT HELPERS
# =========================================================

def create_token(data: Dict[str, Any], expires_delta: timedelta, token_type: str):
    now = datetime.now(timezone.utc)

    payload = data.copy()
    payload.update({
        "type": token_type,
        "iat": now,
        "exp": now + expires_delta,
    })

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(data: Dict[str, Any]):
    return create_token(
        data=data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )


def create_refresh_token(data: Dict[str, Any]):
    return create_token(
        data=data,
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        token_type="refresh",
    )


def decode_token(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except PyJWTError as e:
        raise ValueError(f"Invalid token: {e}")


# =========================================================
# 👤 CURRENT USER VIA COOKIE
# =========================================================

async def get_current_user(access_token: str = Cookie(default=None)):
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing access token",
        )

    try:
        payload = decode_token(access_token)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing subject",
        )

    return {"id": int(user_id)}
