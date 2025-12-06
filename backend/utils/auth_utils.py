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
    data: bijv. {"sub": user_id}
    token_type: "access" of "refresh"
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        raise ValueError(f"Invalid token: {e}")


# =========================================
# 🔑 Nieuw: Correcte user-auth via cookies
# =========================================

async def get_current_user(request: Request):
    """
    Haalt een geldige JWT access token uit 'access_token' cookie.
    Decodet token, haalt user_id uit 'sub', en geeft {"id": user_id} terug.

    Werkt 100% correct met Next.js wanneer fetch() gebruikt:
        credentials: "include"
    """

    # 1️⃣ Token ophalen uit cookies
    token = request.cookies.get("access_token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing access token",
        )

    # 2️⃣ JWT decoderen
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    # 3️⃣ Token type check
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
        )

    # 4️⃣ User ID ophalen
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing user_id",
        )

    # 5️⃣ Return value (optioneel: haal volledige user uit DB)
    return {"id": int(user_id)}
