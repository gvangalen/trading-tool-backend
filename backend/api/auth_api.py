import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Response, Cookie
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import (
    verify_password,
    hash_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,   # ⬅️ leest JWT uit access_token-cookie
)

# =========================================================
# ⚙️ Router
# =========================================================
router = APIRouter()
logger = logging.getLogger(__name__)

# =========================================================
# 📦 Request & Response Models
# =========================================================

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RegisterRequest(BaseModel):
    first_name: str
    last_name: Optional[str] = None
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str
    is_active: bool
    first_name: Optional[str]
    last_name: Optional[str]


# =========================================================
# 🔧 Helpers
# =========================================================

def _row_to_user(row):
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


def _get_user_by_id(user_id: int):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE id = %s
        """, (user_id,))
        return _row_to_user(cur.fetchone())


def _get_user_by_email(email: str):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE email = %s
        """, (email,))
        return _row_to_user(cur.fetchone())


# =========================================================
# 🍪 COOKIE SETTINGS
# =========================================================

COOKIE_SETTINGS = dict(
    httponly=True,
    secure=False,   # True zodra je HTTPS hebt
    samesite="lax",
    path="/",
)


# =========================================================
# 🧪 REGISTER
# =========================================================

@router.post("/auth/register", response_model=UserOut)
def register_user(body: RegisterRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Geen databaseverbinding")

    # Duplicate email check
    if _get_user_by_email(body.email):
        raise HTTPException(400, "E-mail bestaat al")

    with conn.cursor() as cur:
        # eerste gebruiker wordt admin
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
        except Exception:
            conn.rollback()
            raise HTTPException(400, "Gebruiker kan niet worden aangemaakt")

    return UserOut(
        id=row[0],
        email=row[1],
        role=row[2],
        is_active=row[3],
        first_name=row[4],
        last_name=row[5],
    )


# =========================================================
# 🔐 LOGIN  (cookies)
# =========================================================

@router.post("/auth/login")
def login(body: LoginRequest, response: Response):
    user = _get_user_by_email(body.email)

    if not user or not user["is_active"]:
        raise HTTPException(401, "Onjuiste inloggegevens")

    if not verify_password(body.password, user["password_hash"]):
        raise HTTPException(401, "Onjuiste inloggegevens")

    payload = {"sub": str(user["id"]), "role": user["role"]}

    access_token = create_access_token(payload)
    refresh_token = create_refresh_token(payload)

    # ✅ Cookies zetten
    response.set_cookie(
        "access_token",
        access_token,
        max_age=60 * 60,   # 1 uur
        **COOKIE_SETTINGS,
    )
    response.set_cookie(
        "refresh_token",
        refresh_token,
        max_age=60 * 60 * 24 * 7,  # 7 dagen
        **COOKIE_SETTINGS,
    )

    # Update last_login
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET last_login_at=%s WHERE id=%s",
            (datetime.utcnow(), user["id"]),
        )
        conn.commit()

    return {
        "success": True,
        "user": {
            "id": user["id"],
            "email": user["email"],
            "role": user["role"],
            "first_name": user["first_name"],
            "last_name": user["last_name"],
        },
    }


# =========================================================
# 🔁 REFRESH TOKEN (cookies)
# =========================================================

@router.post("/auth/refresh")
def refresh_token(
    response: Response,
    refresh_token: Optional[str] = Cookie(default=None),
):
    if not refresh_token:
        raise HTTPException(401, "Geen refresh token")

    try:
        payload = decode_token(refresh_token)
    except ValueError:
        raise HTTPException(401, "Refresh token ongeldig")

    if payload.get("type") != "refresh":
        raise HTTPException(401, "Verkeerd token type")

    user = _get_user_by_id(int(payload["sub"]))
    if not user:
        raise HTTPException(404, "Gebruiker niet gevonden")

    new_access = create_access_token({"sub": str(user["id"]), "role": user["role"]})

    resp = JSONResponse({"success": True})
    resp.set_cookie(
        "access_token",
        new_access,
        max_age=60 * 60,
        **COOKIE_SETTINGS,
    )
    return resp


# =========================================================
# 🚪 LOGOUT
# =========================================================

@router.post("/auth/logout")
def logout(response: Response):
    resp = JSONResponse({"success": True})
    resp.delete_cookie("access_token", path="/")
    resp.delete_cookie("refresh_token", path="/")
    return resp


# =========================================================
# 👤 /auth/me — via cookie-JWT
# =========================================================

@router.get("/auth/me", response_model=UserOut)
async def get_me(current_user: dict = Depends(get_current_user)):
    user = _get_user_by_id(current_user["id"])
    if not user:
        raise HTTPException(404, "Gebruiker niet gevonden")

    return UserOut(
        id=user["id"],
        email=user["email"],
        role=user["role"],
        is_active=user["is_active"],
        first_name=user["first_name"],
        last_name=user["last_name"],
    )
