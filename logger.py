"""
Логгер на базе loguru.
Формат: [HH:MM:SS] emoji сообщение — как в simulation script.
"""
import sys
from pathlib import Path

from loguru import logger as _logger

from config import LOG_PATH

_configured = False


def _configure() -> None:
    global _configured
    if _configured:
        return
    _logger.remove()
    # Консоль — цветной вывод
    _logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    )
    # Файл — ротация 10 МБ, хранение 30 дней
    _logger.add(
        LOG_PATH,
        rotation="10 MB",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )
    _configured = True


def get_logger():
    _configure()
    return _logger


def get_log_tail(n: int = 50) -> str:
    """Возвращает последние n строк лог-файла."""
    p = Path(LOG_PATH)
    if not p.exists():
        return "(лог пуст)"
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])
