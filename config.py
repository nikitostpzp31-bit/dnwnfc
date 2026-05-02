"""
Конфигурация из переменных окружения (.env).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN: str     = os.getenv("TELEGRAM_TOKEN", "")
OWNER_TELEGRAM_ID: int  = int(os.getenv("OWNER_TELEGRAM_ID", "0"))
FERNET_KEY: str         = os.getenv("FERNET_KEY", "")

DB_PATH: str  = os.getenv("DB_PATH", "apple_bot.db")
LOG_PATH: str = os.getenv("LOG_PATH", "apple_bot.log")

# false = показывает браузер (рекомендуется при первом входе)
# true  = скрытый режим (для сервера)
HEADLESS: bool = os.getenv("HEADLESS", "false").lower() == "true"

# Интервал мониторинга в секундах (по умолчанию 5 минут)
MONITOR_INTERVAL: int = int(os.getenv("MONITOR_INTERVAL", "300"))
