import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Response, Cookie
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import (
    verify_password,
    hash_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,      # ✅ DE ENIGE GEBRUIKTE AUTH FUNCTIE
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# =========================================
# 🔢 SCHEMAS
# =========================================

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RegisterRequest(BaseModel):
    first_name: str
    last_name: Optional[str] = None
    email: EmailStr
    password: str
    role: Optional[str] = None   # NIET ingelezen, maar blijft voor schema


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str
    is_active: bool
    first_name: Optional[str] = None
    last_name: Optional[str] = None


# =========================================
# 🔎 Helpers
# =========================================

def _row_to_user(row) -> Optional[dict]:
    if not row:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "password_hash": row[2],
        "role": row[3],
        "is_active": row[4],
        "first_name": row[5],
        "last_name": row[6],
    }


def _get_user_by_id(user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE id = %s
        """, (user_id,))
        row = cur.fetchone()
    return _row_to_user(row)


def _get_user_by_email(email: str) -> Optional[dict]:
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE email = %s
        """, (email,))
        row = cur.fetchone()
    return _row_to_user(row)


# =========================================
# 🧪 REGISTER
# =========================================

@router.post("/register", response_model=UserOut)
def register_user(body: RegisterRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users")
        (count,) = cur.fetchone()
        role = "admin" if count == 0 else "user"

        password_hash = hash_password(body.password)

        try:
            cur.execute("""
                INSERT INTO users (email, password_hash, role, is_active, first_name, last_name)
                VALUES (%s, %s, %s, TRUE, %s, %s)
                RETURNING id, email, role, is_active, first_name, last_name
            """, (body.email, password_hash, role, body.first_name, body.last_name))

            row = cur.fetchone()
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise HTTPException(400, "Gebruiker aanmaken mislukt") from e

    return UserOut(
        id=row[0],
        email=row[1],
        role=row[2],
        is_active=row[3],
        first_name=row[4],
        last_name=row[5],
    )


# =========================================
# 🔐 LOGIN
# =========================================

@router.post("/login")
def login(body: LoginRequest, response: Response):
    user = _get_user_by_email(body.email)
    if not user or not user["is_active"]:
        raise HTTPException(401, "Onjuiste inloggegevens")

    if not verify_password(body.password, user["password_hash"]):
        raise HTTPException(401, "Onjuiste inloggegevens")

    payload = {"sub": str(user["id"]), "role": user["role"]}
    access_token = create_access_token(payload)
    refresh_token = create_refresh_token(payload)

    # Cookies zetten
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,         # ⚠️ In productie: True
        samesite="lax",
        max_age=3600,
        path="/",
    )

    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=3600 * 24 * 7,
        path="/",
    )

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET last_login_at = %s WHERE id = %s",
            (datetime.utcnow(), user["id"]),
        )
        conn.commit()

    return {"success": True, "user": user}


# =========================================
# 🚪 LOGOUT
# =========================================

@router.post("/logout")
def logout(response: Response):
    response = JSONResponse({"success": True})
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    return response


# =========================================
# 🔁 REFRESH TOKEN
# =========================================

@router.post("/refresh")
def refresh_token(response: Response, refresh_token: Optional[str] = Cookie(default=None)):
    if not refresh_token:
        raise HTTPException(401, "Geen refresh token")

    try:
        payload = decode_token(refresh_token)
    except:
        raise HTTPException(401, "Ongeldige refresh token")

    if payload.get("type") != "refresh":
        raise HTTPException(401, "Onjuist token type")

    user = _get_user_by_id(int(payload.get("sub")))
    if not user:
        raise HTTPException(401, "Gebruiker niet gevonden")

    new_access = create_access_token({"sub": str(user["id"]), "role": user["role"]})

    response = JSONResponse({"success": True})
    response.set_cookie(
        key="access_token",
        value=new_access,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=3600,
        path="/"
    )
    return response


# =========================================
# 🧑 CURRENT USER
# =========================================

@router.get("/me", response_model=UserOut)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserOut(
        id=current_user["id"],
        email=current_user["email"],
        role=current_user["role"],
        is_active=current_user["is_active"],
        first_name=current_user.get("first_name"),
        last_name=current_user.get("last_name"),
    )
