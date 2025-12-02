import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import jwt, JWTError
from passlib.context import CryptContext

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
