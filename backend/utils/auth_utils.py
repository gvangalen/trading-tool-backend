import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Request, HTTPException, status

# =========================================
# 🔐 Config
# =========================================

SECRET_KEY = os.getenv("JWT_SECRET", "CHANGE_ME_IN_PRODUCTION")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# =========================================
# 🧂 Password helpers
# =========================================

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


# =========================================
# 🎫 JWT helpers
# =========================================

def create_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta],
    token_type: str = "access",
) -> str:
    """
    data: bijv {"sub": user_id, "role": "..."}
    """
    to_encode = data.copy()
    to_encode["type"] = token_type
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode["exp"] = expire

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_access_token(data: Dict[str, Any]) -> str:
    return create_token(
        data=data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )


def create_refresh_token(data: Dict[str, Any]) -> str:
    return create_token(
        data=data,
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        token_type="refresh",
    )


def decode_token(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        raise ValueError(f"Invalid token: {e}")


# =========================================
# 🔑 Unified get_current_user (cookie-based)
# =========================================
#
# Dit is de ENIGE juiste functie die ALLE API-bestanden moeten gebruiken.
# Geen ?user_id hacks meer, geen dubbele versies.
# Next.js stuurt cookies automatisch met credentials: "include"
#
# =========================================

async def get_current_user(request: Request) -> dict:
    """
    Leest de JWT access token uit HttpOnly cookie 'access_token'.
    Valideert token. Returned minimaal: {"id": int}
    """
    token = request.cookies.get("access_token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing access token",
        )

    # JWT decoderen
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    # Type check
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
        )

    # User ID ophalen
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing user_id",
        )

    return {"id": int(user_id)}
