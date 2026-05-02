"""Entry point for Apple ID Monitor Bot."""
import asyncio
import os
import sys

import db
from bot import create_bot_and_dispatcher
from config import FERNET_KEY, MONITOR_INTERVAL, TELEGRAM_TOKEN
from logger import get_logger

logger = get_logger()


async def _force_session(bot):
    """Force-acquire Telegram polling session, displacing any existing connection."""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    for attempt in range(1, 121):
        try:
            await bot.get_updates(offset=-1, timeout=0)
            logger.info(f"Session acquired (attempt {attempt})")
            return
        except Exception as e:
            if "Conflict" in str(e):
                if attempt % 10 == 1:
                    logger.warning(f"Session conflict ({attempt}/120), retrying...")
                await asyncio.sleep(5)
            else:
                # Non-conflict error — just proceed
                return


async def run_bot():
    db.init_db()
    bot, dp = create_bot_and_dispatcher()

    async def on_startup(**kwargs):
        if db.get_config("monitor", "off") == "on":
            from bot import _monitor_loop
            logger.info("Restoring monitor task...")
            asyncio.create_task(_monitor_loop())

    dp.startup.register(on_startup)

    while True:
        try:
            await _force_session(bot)
            logger.info("Bot polling started")
            await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
            break  # clean exit (KeyboardInterrupt etc.)

        except Exception as e:
            if "Conflict" in str(e):
                logger.warning(f"Polling conflict, retrying in 30s: {e}")
                await asyncio.sleep(30)
            else:
                logger.error(f"Fatal error: {e}")
                raise


async def main():
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN not set")
        sys.exit(1)
    if not FERNET_KEY:
        from cryptography.fernet import Fernet
        logger.warning(
            f"FERNET_KEY not set — passwords stored unencrypted. "
            f"Generate one: FERNET_KEY={Fernet.generate_key().decode()}"
        )

    logger.info("=" * 50)
    logger.info(f"Apple ID Monitor Bot starting (PID={os.getpid()})")
    logger.info(f"Monitor interval: {MONITOR_INTERVAL}s")
    logger.info("=" * 50)

    await run_bot()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
