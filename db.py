"""
SQLite база данных.
Хранит: настройки бота, известные устройства, журнал действий.
Пароли и ответы на вопросы шифруются через Fernet.
"""
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

from cryptography.fernet import Fernet, InvalidToken

from config import DB_PATH, FERNET_KEY
from logger import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Шифрование
# ---------------------------------------------------------------------------

def _get_fernet() -> Optional[Fernet]:
    if not FERNET_KEY:
        return None
    try:
        return Fernet(FERNET_KEY.encode())
    except Exception:
        return None


def _encrypt(value: str) -> str:
    f = _get_fernet()
    if f is None:
        return value
    return f.encrypt(value.encode()).decode()


def _decrypt(value: str) -> str:
    f = _get_fernet()
    if f is None:
        return value
    try:
        return f.decrypt(value.encode()).decode()
    except (InvalidToken, Exception):
        return value


# Расширенный список чувствительных ключей (включая 3-й вопрос)
_SENSITIVE_KEYS = {"password", "q1_answer", "q2_answer", "q3_answer"}


# ---------------------------------------------------------------------------
# Соединение
# ---------------------------------------------------------------------------

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Инициализация схемы
# ---------------------------------------------------------------------------

def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bot_config (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS known_devices (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL UNIQUE,
                model      TEXT DEFAULT '',
                imei       TEXT DEFAULT '',
                first_seen TEXT DEFAULT (datetime('now')),
                last_seen  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS action_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                action     TEXT NOT NULL,
                details    TEXT DEFAULT '',
                result     TEXT DEFAULT '',
                timestamp  TEXT DEFAULT (datetime('now'))
            );
        """)
    logger.info("БД инициализирована")


# ---------------------------------------------------------------------------
# Bot config — хранит email, пароль, вопросы, флаги
# ---------------------------------------------------------------------------

def set_config(key: str, value: str) -> None:
    """Сохраняет значение (чувствительные данные шифруются)."""
    stored = _encrypt(value) if key in _SENSITIVE_KEYS else value
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO bot_config(key, value, updated_at) VALUES(?, ?, datetime('now')) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, stored),
        )


def get_config(key: str, default: str = "") -> str:
    """Читает значение (расшифровывает если нужно)."""
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM bot_config WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    return _decrypt(row[0]) if key in _SENSITIVE_KEYS else row[0]


def get_setup() -> dict:
    """Возвращает полный конфиг бота."""
    return {
        "email":       get_config("email"),
        "password":    get_config("password"),
        "q1_text":     get_config("q1_text"),
        "q1_answer":   get_config("q1_answer"),
        "q2_text":     get_config("q2_text"),
        "q2_answer":   get_config("q2_answer"),
        "q3_text":     get_config("q3_text"),
        "q3_answer":   get_config("q3_answer"),
        "birthdate":   get_config("birthdate"),
        "autoprotect": get_config("autoprotect", "off"),
        "monitor":     get_config("monitor", "off"),
        "last_login":  get_config("last_login", ""),
        "last_chpass": get_config("last_chpass", ""),
    }


def is_setup_complete() -> bool:
    s = get_setup()
    return bool(s["email"] and s["password"])


# ---------------------------------------------------------------------------
# Known devices — для обнаружения новых устройств
# ---------------------------------------------------------------------------

def get_known_devices() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM known_devices ORDER BY first_seen"
        ).fetchall()
    return [dict(r) for r in rows]


def save_known_device(name: str, model: str = "", imei: str = "") -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO known_devices(name, model, imei, last_seen) VALUES(?, ?, ?, datetime('now')) "
            "ON CONFLICT(name) DO UPDATE SET "
            "last_seen=datetime('now'), "
            "model=COALESCE(NULLIF(excluded.model,''), model), "
            "imei=COALESCE(NULLIF(excluded.imei,''), imei)",
            (name, model, imei),
        )


def clear_known_devices() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM known_devices")


def find_new_devices(current_devices: list[dict]) -> list[dict]:
    """Сравнивает текущий список с известными. Возвращает новые устройства."""
    known = {d["name"].lower() for d in get_known_devices()}
    new_devs = []
    for dev in current_devices:
        name = (dev.get("name") or "").strip()
        if name and name.lower() not in known:
            new_devs.append(dev)
    return new_devs


# ---------------------------------------------------------------------------
# Action log — журнал действий
# ---------------------------------------------------------------------------

def log_action(action: str, details: str = "", result: str = "") -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO action_log(action, details, result) VALUES(?, ?, ?)",
            (action, details, result),
        )


def get_action_log(limit: int = 20) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM action_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]
