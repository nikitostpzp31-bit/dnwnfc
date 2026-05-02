"""
Вспомогательные утилиты.
"""
import re
from datetime import datetime
from typing import Optional


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"[a-zA-Z0-9.%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email.strip()))


def is_valid_date(date_str: str) -> bool:
    """Проверяет формат ДД.ММ.ГГГГ."""
    try:
        datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return True
    except ValueError:
        return False


def validate_apple_password(password: str) -> Optional[str]:
    """
    Проверяет пароль по требованиям Apple ID.
    Возвращает None если пароль валиден, иначе строку с ошибкой.
    """
    if len(password) < 8:
        return "Минимум 8 символов"
    if not re.search(r"[A-Z]", password):
        return "Нужна хотя бы одна заглавная буква (A-Z)"
    if not re.search(r"[a-z]", password):
        return "Нужна хотя бы одна строчная буква (a-z)"
    if not re.search(r"\d", password):
        return "Нужна хотя бы одна цифра (0-9)"
    return None


def mask_email(email: str) -> str:
    """user@example.com → u***@example.com"""
    if not email or "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 1:
        return f"{local}***@{domain}"
    return f"{local[0]}***@{domain}"


def truncate(text: str, max_len: int = 4000) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."
