import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import jwt
from fastapi import HTTPException, Cookie, Depends
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

# =========================================================
# 🔐 JWT CONFIG
# =========================================================

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "super-secret-key-change-me")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# =========================================================
# 🔑 PASSWORD HELPERS
# =========================================================

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False


# =========================================================
# 🧾 JWT HELPERS
# =========================================================

def _create_token(data: Dict[str, Any], expires_delta: timedelta, token_type: str) -> str:
    """
    Genereer een JWT met type: "access" of "refresh".
    """
    to_encode = data.copy()
    now = datetime.now(timezone.utc)

    to_encode.update(
        {
            "type": token_type,
            "iat": now,
            "exp": now + expires_delta,
        }
    )

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(data: Dict[str, Any]) -> str:
    return _create_token(
        data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )


def create_refresh_token(data: Dict[str, Any]) -> str:
    return _create_token(
        data,
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        token_type="refresh",
    )


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decodeer een JWT en geeft payload terug.
    Gooit ValueError bij ongeldig / verlopen token.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("❌ JWT expired")
        raise ValueError("Token is verlopen")
    except jwt.PyJWTError:
        logger.warning("❌ JWT invalid")
        raise ValueError("Token is ongeldig")


# =========================================================
# 👤 CURRENT USER VIA COOKIE
# =========================================================

async def get_current_user(
    access_token: Optional[str] = Cookie(default=None),
) -> Dict[str, Any]:
    """
    Leest de access_token JWT uit de HttpOnly cookie 'access_token'.
    Geen Authorization header, geen Bearer meer nodig.
    """
    if not access_token:
        # Geen cookie → niet ingelogd
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        payload = decode_token(access_token)
    except ValueError:
        # Ongeldig of verlopen token
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Verwacht: type = access
    token_type = payload.get("type")
    if token_type != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")

    user_id = payload.get("sub")
    role = payload.get("role", "user")

    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # Dit object gebruiken we overal (auth_api, onboarding_api, etc.)
    return {
        "id": uid,
        "role": role,
    }
