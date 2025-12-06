import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Request, HTTPException, status  # ✅ nieuw

# =========================================
# 🔐 Config
# =========================================

SECRET_KEY = os.getenv("JWT_SECRET", "CHANGE_ME_IN_PRODUCTION")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# =========================================
# 🔑 Password helpers
# =========================================

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


# =========================================
# 🧾 JWT helpers
# =========================================

def create_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta],
    token_type: str = "access",
) -> str:
    """
    data: bijv. {"sub": user_id, "role": "admin"}
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
# 🔙 Backwards compatible helper
#    voor routes die get_current_user gebruiken
# =========================================

async def get_current_user(request: Request):
    """
    Backwards compatible helper zodat oude imports blijven werken.

    Voor nu:
    - leest `user_id` uit de query (?user_id=...)
    - geeft een simpel user-object terug: {"id": <user_id>}

    Later kun je dit makkelijk aanpassen om:
    - user_id uit JWT / cookies te halen, of
    - de volledige user uit de database op te zoeken.
    """

    user_id = request.query_params.get("user_id")

    if not user_id:
        # Geen user_id mee → beschouwen als niet ingelogd
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    try:
        uid = int(user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid user_id",
        )

    # Voor nu alleen id teruggeven; meer velden kun je later toevoegen
    return {"id": uid}
