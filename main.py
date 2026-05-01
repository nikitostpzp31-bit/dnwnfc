"""
Точка входа. Запуск: python main.py
"""
import asyncio
import signal
import sys

import db
from bot import create_bot_and_dispatcher, notify
from config import FERNET_KEY, MONITOR_INTERVAL, TELEGRAM_TOKEN
from logger import get_logger

logger = get_logger()


def _ensure_fernet_key() -> None:
    """Генерирует FERNET_KEY если не задан и выводит его для .env."""
    if not FERNET_KEY:
        from cryptography.fernet import Fernet
        key = Fernet.generate_key().decode()
        logger.warning(
            "FERNET_KEY не задан в .env. Пароли хранятся без шифрования.\n"
            f"Сгенерируйте ключ и добавьте в .env:\n  FERNET_KEY={key}"
        )


async def main() -> None:
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не задан в .env. Выход.")
        sys.exit(1)

    _ensure_fernet_key()

    logger.info("=" * 50)
    logger.info("🍎 Apple ID Monitor Bot — запуск")
    logger.info(f"   Интервал мониторинга: {MONITOR_INTERVAL} сек")
    logger.info("=" * 50)

    db.init_db()

    bot, dp = create_bot_and_dispatcher()

    # Сбрасываем webhook и вытесняем конкурирующий polling
    logger.info("Сброс webhook...")
    await bot.delete_webhook(drop_pending_updates=True)

    for attempt in range(20):
        try:
            await bot.get_updates(offset=-1, timeout=0)
            logger.info(f"Сессия захвачена (попытка {attempt + 1})")
            break
        except Exception as e:
            if "Conflict" in str(e):
                logger.warning(f"Конфликт сессии, попытка {attempt + 1}/20...")
                await asyncio.sleep(2)
            else:
                break

    # Восстанавливаем мониторинг если был включён до перезапуска
    if db.get_config("monitor", "off") == "on":
        from bot import _monitor_loop
        import asyncio as _asyncio
        logger.info("Восстанавливаю мониторинг после перезапуска...")
        _asyncio.create_task(_monitor_loop())

    logger.info("✅ Бот запущен и ожидает команды")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен (Ctrl+C)")
