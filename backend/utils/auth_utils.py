import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Request, HTTPException, status


# =========================================================
# 🔐 CONFIG
# =========================================================

SECRET_KEY = os.getenv("JWT_SECRET", "CHANGE_ME_IN_PRODUCTION")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
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

def create_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta],
    token_type: str = "access",
) -> str:

    to_encode = data.copy()
    to_encode["type"] = token_type
    to_encode["exp"] = datetime.utcnow() + expires_delta

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


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


# =========================================================
# 🔑 GET CURRENT USER (BEARER TOKEN VERSION)
# =========================================================
#
# Deze methode:
#   ✓ leest "Authorization: Bearer <token>"
#   ✓ valideert token
#   ✓ controleert type == access
#   ✓ geeft {"id": <user_id>}
#
# Alle API endpoints gebruiken deze via Depends(get_current_user)
#
# =========================================================

async def get_current_user(request: Request) -> dict:
    """
    JWT uit Authorization-header halen:
        Authorization: Bearer eyJhbGciOi...
    """

    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    token = auth_header.split(" ")[1].strip()

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
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
            detail="Token missing subject (user id)",
        )

    return {"id": int(user_id)}
