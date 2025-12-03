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
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

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
    role: Optional[str] = "admin"  # eerste user → admin


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
    # row = (id, email, password_hash, role, is_active, first_name, last_name)
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
        cur.execute(
            """
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
    return _row_to_user(row)


def _get_user_by_email(email: str) -> Optional[dict]:
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, email, password_hash, role, is_active, first_name, last_name
            FROM users
            WHERE email = %s
            """,
            (email,),
        )
        row = cur.fetchone()
    return _row_to_user(row)


async def get_current_user(
    access_token: Optional[str] = Cookie(default=None, alias="access_token")
) -> dict:
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Niet ingelogd",
        )

    try:
        payload = decode_token(access_token)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ongeldige of verlopen token",
        )

    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Onjuist token type",
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ongeldige token payload",
        )

    user = _get_user_by_id(int(user_id))
    if not user or not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Gebruiker niet actief of bestaat niet",
        )

    return user


# =========================================
# 🧪 REGISTER (simpel bootstrap)
# =========================================

@router.post("/register")
def register_user(body: RegisterRequest):
    """
    Simpel endpoint om een eerste user aan te maken.
    In productie zou je dit beperken (alleen admin / éénmalig).
    """
    conn = get_db_connection()
    with conn.cursor() as cur:
        # bestaan er al users?
        cur.execute("SELECT COUNT(*) FROM users")
        (count,) = cur.fetchone()

        role = body.role or "user"
        if count == 0:
            # eerste user → admin
            role = "admin"

        password_hash = hash_password(body.password)

        try:
            cur.execute(
                """
                INSERT INTO users (email, password_hash, role, is_active, first_name, last_name)
                VALUES (%s, %s, %s, TRUE, %s, %s)
                RETURNING id, email, role, is_active, first_name, last_name
                """,
                (body.email, password_hash, role, body.first_name, body.last_name),
            )
            row = cur.fetchone()
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.exception("❌ Error bij register_user")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Gebruiker aanmaken mislukt (bestaat hij al?)",
            ) from e

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
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Onjuiste inloggegevens",
        )

    if not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Onjuiste inloggegevens",
        )

    # Tokens maken
    payload = {"sub": str(user["id"]), "role": user["role"]}
    access_token = create_access_token(payload)
    refresh_token = create_refresh_token(payload)

    # Cookies zetten (HttpOnly)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,    # 🔴 in productie → True
        samesite="lax",
        max_age=60 * 60,  # 1 uur
    )

    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=False,    # 🔴 in productie → True
        samesite="lax",
        max_age=60 * 60 * 24 * 7,  # 7 dagen
    )

    # last_login_at updaten
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET last_login_at = %s WHERE id = %s",
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


# =========================================
# 🚪 LOGOUT
# =========================================

@router.post("/logout")
def logout(response: Response):
    # altijd succes, ook als er geen cookies zijn
    response = JSONResponse({"success": True, "message": "Uitgelogd"})
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")
    return response


# =========================================
# 🔁 REFRESH TOKEN
# =========================================

@router.post("/refresh")
def refresh_token(
    response: Response,
    refresh_token: Optional[str] = Cookie(default=None, alias="refresh_token"),
):
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Geen refresh token",
        )

    try:
        payload = decode_token(refresh_token)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ongeldige of verlopen refresh token",
        )

    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Onjuist token type",
        )

    user_id = payload.get("sub")
    user = _get_user_by_id(int(user_id)) if user_id else None
    if not user or not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Gebruiker niet actief of bestaat niet",
        )

    new_payload = {"sub": str(user["id"]), "role": user["role"]}
    access_token = create_access_token(new_payload)

    response = JSONResponse({"success": True})
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,  # 🔴 in productie: True
        samesite="lax",
        max_age=60 * 60,
    )

    return response


# =========================================
# 🙋‍♂️ CURRENT USER
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
