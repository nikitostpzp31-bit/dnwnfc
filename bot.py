"""
Telegram-бот управления Apple ID.
Команды: /setup /login /devices /findmy /erase /changepass /mail /security
         /monitor /autoprotect /tfa /status /cancel
"""
import asyncio
import os
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

import db
from config import MONITOR_INTERVAL, OWNER_TELEGRAM_ID, TELEGRAM_TOKEN
from logger import get_logger
from utils import mask_email, truncate, validate_apple_password

logger = get_logger()
router = Router()

# ---------------------------------------------------------------------------
# Глобальное состояние
# ---------------------------------------------------------------------------

_bot_instance: Optional[Bot] = None
_monitor_task: Optional[asyncio.Task] = None
_tfa_queue: asyncio.Queue = asyncio.Queue()

# ---------------------------------------------------------------------------
# FSM состояния
# ---------------------------------------------------------------------------

class Setup(StatesGroup):
    email     = State()
    password  = State()
    q1_text   = State()
    q1_answer = State()
    q2_text   = State()
    q2_answer = State()
    confirm   = State()


class ChangePass(StatesGroup):
    current = State()
    new1    = State()
    new2    = State()


class NewDeviceAction(StatesGroup):
    change_pass_current = State()
    change_pass_new1    = State()
    change_pass_new2    = State()
    erase_confirm       = State()


class TwoFA(StatesGroup):
    waiting_code = State()


class QuickSetup(StatesGroup):
    waiting_text = State()
    confirm      = State()


# ---------------------------------------------------------------------------
# Охранник — только владелец
# ---------------------------------------------------------------------------

def is_owner(uid: int) -> bool:
    return uid == OWNER_TELEGRAM_ID


async def guard(obj) -> bool:
    if isinstance(obj, Message):
        if not is_owner(obj.from_user.id):
            await obj.answer("⛔ Доступ запрещён.")
            return False
    elif isinstance(obj, CallbackQuery):
        if not is_owner(obj.from_user.id):
            await obj.answer("⛔ Доступ запрещён.", show_alert=True)
            return False
    return True


# ---------------------------------------------------------------------------
# Уведомление владельца
# ---------------------------------------------------------------------------

async def notify(text: str, photo: Optional[bytes] = None) -> None:
    if not _bot_instance:
        return
    try:
        if photo:
            await _bot_instance.send_photo(
                OWNER_TELEGRAM_ID,
                BufferedInputFile(photo, "screen.png"),
                caption=text[:1024],
                parse_mode="HTML",
            )
        else:
            await _bot_instance.send_message(
                OWNER_TELEGRAM_ID, text, parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"[notify] {e}")


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _get_cfg() -> dict:
    return db.get_setup()


def _pw_args() -> dict:
    """Аргументы для всех playwright-функций из конфига."""
    cfg = _get_cfg()
    return {
        "acc_id":    1,
        "email":     cfg["email"],
        "password":  cfg["password"],
        "q1_text":   cfg["q1_text"],
        "q1_answer": cfg["q1_answer"],
        "q2_text":   cfg["q2_text"],
        "q2_answer": cfg["q2_answer"],
        "q3_text":   cfg.get("q3_text", ""),
        "q3_answer": cfg.get("q3_answer", ""),
        "tfa_queue": _tfa_queue,
        "notify_fn": notify,
    }


async def _run_pw(fn, timeout: int = 300, **extra) -> dict:
    """
    Запускает playwright-функцию с таймаутом и обработкой ошибок.
    Базовые аргументы берутся из _pw_args(), extra перекрывает их.
    """
    args = {**_pw_args(), **extra}
    try:
        return await asyncio.wait_for(fn(**args), timeout=timeout)
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"Таймаут {timeout} сек", "screenshot": None}
    except Exception as e:
        logger.error(f"[_run_pw] {e}")
        return {"ok": False, "error": str(e), "screenshot": None}


async def _send_screenshot(target, photo: Optional[bytes], caption: str = "") -> None:
    if not photo:
        return
    try:
        if isinstance(target, Message):
            await target.answer_photo(
                BufferedInputFile(photo, "screen.png"), caption=caption[:1024]
            )
        elif _bot_instance:
            await _bot_instance.send_photo(
                OWNER_TELEGRAM_ID,
                BufferedInputFile(photo, "screen.png"),
                caption=caption[:1024],
            )
    except Exception as e:
        logger.error(f"[screenshot] {e}")


def _fmt_devices(devices: list) -> str:
    """Форматирует список устройств для Telegram."""
    if not devices:
        return "Устройств не найдено."
    lines = []
    for i, d in enumerate(devices, 1):
        name   = d.get("name") or d.get("description") or "—"
        model  = d.get("model") or "—"
        ver    = d.get("version") or ""
        imei   = d.get("imei") or "—"
        status = d.get("status") or ""
        loc    = d.get("location") or ""
        line = f"<b>{i}. {name}</b>\n"
        if model != "—":
            line += f"   📱 Модель: {model}\n"
        if ver:
            line += f"   🔢 Версия: {ver}\n"
        if imei != "—":
            line += f"   🔑 IMEI: <code>{imei}</code>\n"
        if status:
            line += f"   📶 {status}\n"
        if loc:
            line += f"   📍 {loc}\n"
        lines.append(line.strip())
    return "\n\n".join(lines)


def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Устройства"),    KeyboardButton(text="📍 Find My")],
            [KeyboardButton(text="🔑 Сменить пароль"), KeyboardButton(text="📬 Почта")],
            [KeyboardButton(text="🔒 Безопасность"),   KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def yn_kb(yes_data: str, no_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да", callback_data=yes_data),
        InlineKeyboardButton(text="❌ Нет", callback_data=no_data),
    ]])


# ---------------------------------------------------------------------------
# /start  /help
# ---------------------------------------------------------------------------

@router.message(Command("start", "help"))
async def cmd_start(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.clear()
    try:
        db.init_db()
        cfg = _get_cfg()
        setup_ok = db.is_setup_complete()
    except Exception:
        cfg = {"email": "", "monitor": "off", "autoprotect": "off"}
        setup_ok = False

    mon = cfg.get("monitor", "off")
    ap  = cfg.get("autoprotect", "off")
    known = len(db.get_known_devices())

    status = (
        f"📧 Email: <b>{mask_email(cfg['email']) if cfg['email'] else '—'}</b>\n"
        f"🔍 Мониторинг: {'✅ вкл' if mon == 'on' else '⏸ выкл'}\n"
        f"🛡 Автозащита: {'✅ вкл' if ap == 'on' else '⏸ выкл'}\n"
        f"📱 Известных устройств: {known}\n"
    )
    text = (
        "🍎 <b>Apple ID Monitor Bot</b>\n\n"
        f"{status}\n"
        "<b>Команды:</b>\n"
        "/setup — настройка (email, пароль, вопросы)\n"
        "/login — войти в аккаунт\n"
        "/devices — список устройств + IMEI\n"
        "/findmy — локатор устройств\n"
        "/erase [имя] — стереть устройство\n"
        "/changepass — сменить пароль\n"
        "/mail — проверить почту iCloud\n"
        "/security — настройки безопасности\n"
        "/monitor start|stop — мониторинг каждые 5 мин\n"
        "/autoprotect on|off — автозащита при новом устройстве\n"
        "/status — статус бота\n"
        "/tfa [код] — ввести код 2FA\n"
        "/cancel — отменить текущее действие\n"
    )
    if not setup_ok:
        text += "\n⚠️ <b>Сначала выполните /setup</b>"
    await m.answer(text, parse_mode="HTML", reply_markup=main_kb())


@router.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.clear()
    await m.answer("❌ Действие отменено.", reply_markup=main_kb())


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

@router.message(Command("status"))
async def cmd_status(m: Message):
    if not await guard(m):
        return
    cfg = _get_cfg()
    known = db.get_known_devices()
    mon_active = _monitor_task and not _monitor_task.done()
    log = db.get_action_log(5)

    last_login  = cfg.get("last_login", "—") or "—"
    last_chpass = cfg.get("last_chpass", "—") or "—"
    mon_status  = "✅ активен" if mon_active else "⏸ остановлен"
    ap_status   = "✅ вкл" if cfg.get("autoprotect") == "on" else "⏸ выкл"

    lines = [
        "📊 <b>Статус бота</b>\n",
        f"📧 Email: <code>{cfg.get('email','—')}</code>",
        f"🔍 Мониторинг: {mon_status}",
        f"🛡 Автозащита: {ap_status}",
        f"📱 Известных устройств: {len(known)}",
        f"🕐 Последний вход: {last_login}",
        f"🔑 Последняя смена пароля: {last_chpass}",
        f"⏱ Интервал мониторинга: {MONITOR_INTERVAL} сек",
    ]
    if log:
        lines.append("\n<b>Последние действия:</b>")
        for entry in log:
            lines.append(f"  • {entry['action']} — {entry['result']} ({entry['timestamp'][:16]})")

    await m.answer("\n".join(lines), parse_mode="HTML", reply_markup=main_kb())


# ---------------------------------------------------------------------------
# /tfa — ввод кода двухфакторной аутентификации
# ---------------------------------------------------------------------------

@router.message(Command("tfa"))
async def cmd_tfa(m: Message):
    if not await guard(m):
        return
    parts = m.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await m.answer(
            "Использование: <code>/tfa 123456</code>\n"
            "Введите 6-значный код из SMS или приложения Authenticator.",
            parse_mode="HTML",
        )
        return
    code = parts[1].strip()
    if not code.isdigit() or len(code) != 6:
        await m.answer("❌ Код должен состоять ровно из 6 цифр.")
        return
    await _tfa_queue.put(code)
    await m.answer("✅ Код 2FA передан боту.")
    logger.info(f"[tfa] Код получен от пользователя: {code}")


# ---------------------------------------------------------------------------
# /setup — пошаговая настройка (FSM)
# ---------------------------------------------------------------------------

@router.message(Command("setup"))
async def cmd_setup(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.clear()
    await state.set_state(Setup.email)
    await m.answer(
        "⚙️ <b>Настройка Apple ID бота</b>\n\n"
        "Шаг 1/6: Введите ваш Apple ID (email):",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(Setup.email))
async def setup_email(m: Message, state: FSMContext):
    if not await guard(m):
        return
    from utils import is_valid_email
    email = m.text.strip()
    if not is_valid_email(email):
        await m.answer("❌ Некорректный email. Попробуйте ещё раз:")
        return
    await state.update_data(email=email)
    await state.set_state(Setup.password)
    await m.answer(
        "Шаг 2/6: Введите пароль Apple ID:\n"
        "<i>(сообщение будет удалено сразу после отправки)</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(Setup.password))
async def setup_password(m: Message, state: FSMContext):
    if not await guard(m):
        return
    pwd = m.text.strip()
    try:
        await m.delete()
    except Exception:
        pass
    err = validate_apple_password(pwd)
    if err:
        await m.answer(f"❌ {err}\nПопробуйте ещё раз:")
        return
    await state.update_data(password=pwd)
    await state.set_state(Setup.q1_text)
    await m.answer(
        "Шаг 3/6: Введите текст <b>первого</b> контрольного вопроса\n"
        "<i>Пример: 你的理想工作是什么？</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(Setup.q1_text))
async def setup_q1_text(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.update_data(q1_text=m.text.strip())
    await state.set_state(Setup.q1_answer)
    await m.answer(
        f"Шаг 4/6: Введите <b>ответ</b> на первый вопрос:\n"
        f"«{m.text.strip()}»\n\n"
        "<i>Вводите ТОЧНО как в Apple ID — без лишних пробелов</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(Setup.q1_answer))
async def setup_q1_answer(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.update_data(q1_answer=m.text.strip())
    await state.set_state(Setup.q2_text)
    await m.answer(
        "Шаг 5/6: Введите текст <b>второго</b> контрольного вопроса\n"
        "<i>Пример: 你少年时代最好的朋友叫什么名字？</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(Setup.q2_text))
async def setup_q2_text(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.update_data(q2_text=m.text.strip())
    await state.set_state(Setup.q2_answer)
    await m.answer(
        f"Шаг 6/6: Введите <b>ответ</b> на второй вопрос:\n"
        f"«{m.text.strip()}»",
        parse_mode="HTML",
    )


@router.message(StateFilter(Setup.q2_answer))
async def setup_q2_answer(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.update_data(q2_answer=m.text.strip())
    data = await state.get_data()
    await state.set_state(Setup.confirm)
    text = (
        "✅ <b>Проверьте данные перед сохранением:</b>\n\n"
        f"📧 Email: <code>{data['email']}</code>\n"
        f"🔑 Пароль: <code>{'*' * len(data['password'])}</code>\n"
        f"❓ Вопрос 1: {data['q1_text']}\n"
        f"   Ответ: <code>{data['q1_answer']}</code>\n"
        f"❓ Вопрос 2: {data['q2_text']}\n"
        f"   Ответ: <code>{data['q2_answer']}</code>\n\n"
        "Сохранить?"
    )
    await m.answer(text, parse_mode="HTML", reply_markup=yn_kb("setup_save", "setup_cancel"))


@router.callback_query(F.data == "setup_save")
async def setup_save(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    data = await state.get_data()
    await state.clear()
    db.set_config("email",     data["email"])
    db.set_config("password",  data["password"])
    db.set_config("q1_text",   data["q1_text"])
    db.set_config("q1_answer", data["q1_answer"])
    db.set_config("q2_text",   data["q2_text"])
    db.set_config("q2_answer", data["q2_answer"])
    await cb.message.answer(
        f"✅ <b>Настройка сохранена!</b>\n"
        f"Email: <code>{data['email']}</code>\n\n"
        "Теперь можно использовать /login /devices /findmy",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )
    await cb.answer()
    logger.info(f"[setup] Настройка сохранена для {data['email']}")


@router.callback_query(F.data == "setup_cancel")
async def setup_cancel_cb(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    await state.clear()
    await cb.message.answer("❌ Настройка отменена.", reply_markup=main_kb())
    await cb.answer()


# ---------------------------------------------------------------------------
# /quicksetup — добавление аккаунта одним сообщением
# ---------------------------------------------------------------------------

def _parse_account_text(text: str) -> dict:
    """
    Парсит свободный текст с данными аккаунта.
    Поддерживает форматы:
      почта - user@example.com
      пароль - Qwert4291
      1 вопрос - текст вопроса - ответ
      дата - 21/03/1976
    """
    import re
    result = {
        "email": "", "password": "", "birthdate": "",
        "q1_text": "", "q1_answer": "",
        "q2_text": "", "q2_answer": "",
        "q3_text": "", "q3_answer": "",
    }

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]

    for line in lines:
        ll = line.lower()

        # Email
        if any(k in ll for k in ["почта", "email", "mail", "логин", "login"]):
            m = re.search(r"[\w.%+\-]+@[\w.\-]+\.[a-z]{2,}", line, re.I)
            if m:
                result["email"] = m.group(0)

        # Пароль
        elif any(k in ll for k in ["пароль", "password", "pass"]):
            parts = re.split(r"\s*[-–—:]\s*", line, maxsplit=1)
            if len(parts) == 2:
                result["password"] = parts[1].strip()

        # Дата рождения
        elif any(k in ll for k in ["дата", "date", "рожден", "birthday", "birth"]):
            m = re.search(r"(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})", line)
            if m:
                d, mo, y = m.group(1), m.group(2), m.group(3)
                if len(y) == 2:
                    y = "19" + y
                result["birthdate"] = f"{d.zfill(2)}.{mo.zfill(2)}.{y}"

        # Вопросы — формат: "1 вопрос - текст вопроса - ответ"
        # или "вопрос 1 - текст - ответ"
        elif re.search(r"(вопрос|question)\s*[1-3]|[1-3]\s*(вопрос|question)", ll):
            # Определяем номер вопроса
            num_m = re.search(r"[1-3]", ll)
            num = int(num_m.group(0)) if num_m else 1

            # Убираем префикс "N вопрос" или "вопрос N"
            cleaned = re.sub(
                r"^.*?(вопрос|question)\s*[1-3]\s*[-–—:]\s*|^[1-3]\s*(вопрос|question)\s*[-–—:]\s*",
                "", line, flags=re.I
            ).strip()

            # Разбиваем оставшееся на "текст вопроса - ответ"
            # Ответ — последний сегмент после последнего разделителя
            parts = re.split(r"\s*[-–—]\s*", cleaned)
            if len(parts) >= 2:
                q_text  = " - ".join(parts[:-1]).strip()
                q_answer = parts[-1].strip()
            elif len(parts) == 1:
                q_text   = parts[0].strip()
                q_answer = ""
            else:
                continue

            if num == 1:
                result["q1_text"]   = q_text
                result["q1_answer"] = q_answer
            elif num == 2:
                result["q2_text"]   = q_text
                result["q2_answer"] = q_answer
            elif num == 3:
                result["q3_text"]   = q_text
                result["q3_answer"] = q_answer

    return result


@router.message(Command("quicksetup"))
async def cmd_quicksetup(m: Message, state: FSMContext):
    if not await guard(m):
        return
    await state.set_state(QuickSetup.waiting_text)
    await m.answer(
        "📋 <b>Быстрое добавление аккаунта</b>\n\n"
        "Отправьте данные в любом формате, например:\n\n"
        "<code>почта - user@icloud.com\n"
        "пароль - Qwert4291\n"
        "1 вопрос - 你少年时代最好的朋友叫什么名字 - py777\n"
        "2 вопрос - 你的理想工作是什么 - gz777\n"
        "3 вопрос - 你的父母是在哪里认识的 - fm777\n"
        "дата - 21/03/1976</code>\n\n"
        "⚠️ <i>Сообщение будет удалено сразу после отправки</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(QuickSetup.waiting_text))
async def quicksetup_text(m: Message, state: FSMContext):
    if not await guard(m):
        return
    # Удаляем сообщение с паролем немедленно
    try:
        await m.delete()
    except Exception:
        pass

    parsed = _parse_account_text(m.text or "")

    if not parsed["email"]:
        await m.answer(
            "❌ Email не найден. Попробуйте ещё раз — отправьте /quicksetup",
            reply_markup=main_kb(),
        )
        await state.clear()
        return

    await state.update_data(parsed=parsed)
    await state.set_state(QuickSetup.confirm)

    # Показываем что распознали (без пароля в открытом виде)
    q_lines = []
    for i, (qt, qa) in enumerate([
        (parsed["q1_text"], parsed["q1_answer"]),
        (parsed["q2_text"], parsed["q2_answer"]),
        (parsed["q3_text"], parsed["q3_answer"]),
    ], 1):
        if qt:
            q_lines.append(f"❓ Вопрос {i}: {qt[:40]}…\n   Ответ: <code>{qa}</code>")

    text = (
        "✅ <b>Распознано — проверьте данные:</b>\n\n"
        f"📧 Email: <code>{parsed['email']}</code>\n"
        f"🔑 Пароль: <code>{'*' * len(parsed['password'])}</code>\n"
        f"📅 Дата: {parsed['birthdate'] or '—'}\n"
        + ("\n" + "\n".join(q_lines) if q_lines else "\n❓ Вопросы: не найдены")
        + "\n\nСохранить?"
    )
    await m.answer(
        text,
        parse_mode="HTML",
        reply_markup=yn_kb("quicksetup_save", "quicksetup_cancel"),
    )


@router.callback_query(F.data == "quicksetup_save")
async def quicksetup_save(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    data = await state.get_data()
    parsed = data.get("parsed", {})
    await state.clear()

    # Сохраняем в конфиг (пароль и ответы шифруются Fernet)
    db.set_config("email",     parsed["email"])
    db.set_config("password",  parsed["password"])
    db.set_config("q1_text",   parsed["q1_text"])
    db.set_config("q1_answer", parsed["q1_answer"])
    db.set_config("q2_text",   parsed["q2_text"])
    db.set_config("q2_answer", parsed["q2_answer"])
    # Третий вопрос сохраняем в отдельные ключи
    if parsed.get("q3_text"):
        db.set_config("q3_text",   parsed["q3_text"])
        db.set_config("q3_answer", parsed["q3_answer"])
    if parsed.get("birthdate"):
        db.set_config("birthdate", parsed["birthdate"])

    db.log_action("quicksetup", parsed["email"], "ok")
    logger.info(f"[quicksetup] Аккаунт сохранён: {parsed['email']}")

    await cb.message.answer(
        f"✅ <b>Аккаунт сохранён!</b>\n"
        f"📧 <code>{parsed['email']}</code>\n\n"
        "Теперь: /login → /monitor start",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )
    await cb.answer()


@router.callback_query(F.data == "quicksetup_cancel")
async def quicksetup_cancel(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    await state.clear()
    await cb.message.answer("❌ Отменено.", reply_markup=main_kb())
    await cb.answer()


# ---------------------------------------------------------------------------
# /login — ручной вход
# ---------------------------------------------------------------------------

@router.message(Command("login"))
async def cmd_login(m: Message):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await m.answer("🔄 Выполняю вход в Apple ID…\n<i>~1-2 минуты</i>", parse_mode="HTML")
    from apple_automation import apple_signin, _get_browser, _new_page
    cfg = _get_cfg()
    pw, ctx = await _get_browser(1)
    page = await _new_page(ctx)
    try:
        r = await asyncio.wait_for(
            apple_signin(
                page,
                cfg["email"], cfg["password"],
                cfg["q1_text"], cfg["q1_answer"],
                cfg["q2_text"], cfg["q2_answer"],
                _tfa_queue, notify,
                cfg.get("q3_text", ""), cfg.get("q3_answer", ""),
            ),
            timeout=180,
        )
        if r["ok"]:
            db.set_config("last_login", datetime.now().strftime("%d.%m.%Y %H:%M"))
            db.log_action("login", cfg["email"], "ok")
            await m.answer(
                "✅ <b>Вход выполнен успешно!</b>\n\n"
                "Теперь доступны: /devices /findmy /changepass /mail",
                parse_mode="HTML",
                reply_markup=main_kb(),
            )
        else:
            db.log_action("login", cfg["email"], f"fail: {r['error']}")
            await m.answer(f"❌ Ошибка входа: {r['error']}", reply_markup=main_kb())
            if r.get("screenshot"):
                await _send_screenshot(m, r["screenshot"], "Ошибка на шаге: вход")
    except asyncio.TimeoutError:
        await m.answer("❌ Таймаут входа (180 сек). Попробуйте /login ещё раз.")
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# /devices — список устройств с IMEI
# ---------------------------------------------------------------------------

@router.message(Command("devices"))
@router.message(F.text == "📱 Устройства")
async def cmd_devices(m: Message):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await m.answer("🔄 Загружаю список устройств…\n<i>~2-3 минуты</i>", parse_mode="HTML")
    from apple_automation import get_devices
    r = await _run_pw(get_devices, timeout=240)
    if not r["ok"]:
        await m.answer(f"❌ Ошибка: {r['error']}", reply_markup=main_kb())
        if r.get("screenshot"):
            await _send_screenshot(m, r["screenshot"], "Ошибка на шаге: devices")
        return
    devices = r["devices"]
    # Сохраняем как известные
    for d in devices:
        name = d.get("name", "")
        if name:
            db.save_known_device(name, d.get("model", ""), d.get("imei", ""))
    db.log_action("devices", f"count={len(devices)}", "ok")
    text = f"📱 <b>Устройства аккаунта ({len(devices)}):</b>\n\n{_fmt_devices(devices)}"
    await m.answer(text, parse_mode="HTML", reply_markup=main_kb())


# ---------------------------------------------------------------------------
# /findmy — Find My + обнаружение новых устройств
# ---------------------------------------------------------------------------

@router.message(Command("findmy"))
@router.message(F.text == "📍 Find My")
async def cmd_findmy(m: Message):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await m.answer("🔄 Загружаю Find My…\n<i>~2-3 минуты</i>", parse_mode="HTML")
    from apple_automation import get_findmy_devices
    r = await _run_pw(get_findmy_devices, timeout=240)
    if not r["ok"]:
        await m.answer(f"❌ Ошибка: {r['error']}", reply_markup=main_kb())
        if r.get("screenshot"):
            await _send_screenshot(m, r["screenshot"], "Ошибка на шаге: findmy")
        return
    devices = r["devices"]
    # Проверяем новые устройства
    new_devs = db.find_new_devices(devices)
    # Сохраняем все как известные
    for d in devices:
        name = d.get("name", "")
        if name:
            db.save_known_device(name, d.get("model", ""), d.get("imei", ""))
    db.log_action("findmy", f"count={len(devices)},new={len(new_devs)}", "ok")
    text = f"📍 <b>Find My — устройства ({len(devices)}):</b>\n\n{_fmt_devices(devices)}"
    await m.answer(text, parse_mode="HTML", reply_markup=main_kb())
    if new_devs:
        await _alert_new_devices(m.chat.id, new_devs)


# ---------------------------------------------------------------------------
# Алерт о новом устройстве + inline-кнопки
# ---------------------------------------------------------------------------

async def _alert_new_devices(chat_id: int, new_devs: list) -> None:
    """Отправляет красный алерт о новых устройствах с кнопками действий."""
    for dev in new_devs:
        name   = dev.get("name", "—")
        model  = dev.get("model", "—")
        imei   = dev.get("imei", "—")
        loc    = dev.get("location", "—")
        status = dev.get("status", "")
        text = (
            "🚨🚨🚨 <b>НОВОЕ УСТРОЙСТВО В АККАУНТЕ!</b> 🚨🚨🚨\n\n"
            f"📱 Имя: <b>{name}</b>\n"
            f"📱 Модель: {model}\n"
            f"🔑 IMEI: <code>{imei}</code>\n"
            f"📍 Местоположение: {loc}\n"
            f"📶 Статус: {status}\n\n"
            "⚠️ Выберите действие:"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🔑 Сменить пароль",
                callback_data=f"newdev_chpwd_{name[:30]}",
            )],
            [InlineKeyboardButton(
                text="💥 Стереть устройство",
                callback_data=f"newdev_erase_{name[:30]}",
            )],
            [InlineKeyboardButton(
                text="✅ Это моё устройство",
                callback_data=f"newdev_ok_{name[:30]}",
            )],
        ])
        if _bot_instance:
            await _bot_instance.send_message(
                chat_id, text, parse_mode="HTML", reply_markup=kb
            )
        logger.info(f"[alert] Новое устройство: {name} | IMEI: {imei}")


@router.callback_query(F.data.startswith("newdev_ok_"))
async def cb_newdev_ok(cb: CallbackQuery):
    if not await guard(cb):
        return
    name = cb.data.replace("newdev_ok_", "")
    db.save_known_device(name)
    db.log_action("newdev_trusted", name, "ok")
    await cb.message.answer(f"✅ Устройство «{name}» добавлено в список доверенных.")
    await cb.answer()


@router.callback_query(F.data.startswith("newdev_chpwd_"))
async def cb_newdev_chpwd(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    name = cb.data.replace("newdev_chpwd_", "")
    await state.update_data(threat_device=name)
    await state.set_state(NewDeviceAction.change_pass_current)
    await cb.message.answer(
        f"🔑 Смена пароля из-за устройства <b>{name}</b>\n\n"
        "Введите <b>текущий пароль</b> Apple ID:\n"
        "<i>(сообщение будет удалено)</i>",
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("newdev_erase_"))
async def cb_newdev_erase(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    name = cb.data.replace("newdev_erase_", "")
    await state.update_data(erase_device=name)
    await state.set_state(NewDeviceAction.erase_confirm)
    await cb.message.answer(
        f"💥 Подтвердите стирание устройства <b>{name}</b>?\n"
        "⚠️ <b>Это действие необратимо!</b>",
        parse_mode="HTML",
        reply_markup=yn_kb(f"erase_confirm_{name[:30]}", "erase_cancel"),
    )
    await cb.answer()


# ---------------------------------------------------------------------------
# /erase [device_name]
# ---------------------------------------------------------------------------

@router.message(Command("erase"))
async def cmd_erase(m: Message, state: FSMContext):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    parts = m.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await m.answer(
            "Использование: <code>/erase имя_устройства</code>\n"
            "Пример: <code>/erase iPhone Никиты</code>",
            parse_mode="HTML",
        )
        return
    device_name = parts[1].strip()
    await state.update_data(erase_device=device_name)
    await m.answer(
        f"💥 Стереть устройство <b>{device_name}</b>?\n"
        "⚠️ <b>Это действие необратимо!</b>",
        parse_mode="HTML",
        reply_markup=yn_kb(f"erase_confirm_{device_name[:30]}", "erase_cancel"),
    )


@router.callback_query(F.data.startswith("erase_confirm_"))
async def cb_erase_confirm(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    device_name = cb.data.replace("erase_confirm_", "")
    await cb.message.answer(
        f"🔄 Стираю устройство <b>{device_name}</b>…\n<i>~2-3 минуты</i>",
        parse_mode="HTML",
    )
    await cb.answer()
    from apple_automation import erase_findmy_device
    r = await _run_pw(erase_findmy_device, timeout=240, device_name=device_name)
    await state.clear()
    if r["ok"]:
        db.log_action("erase", device_name, "ok")
        await cb.message.answer(
            f"✅ <b>Устройство «{device_name}» стёрто!</b>\n\n"
            "Проверьте /mail для подтверждения от Apple.",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    else:
        db.log_action("erase", device_name, f"fail: {r['error']}")
        await cb.message.answer(
            f"❌ Ошибка стирания: {r['error']}", reply_markup=main_kb()
        )
        if r.get("screenshot"):
            await _send_screenshot(cb.message, r["screenshot"], "Ошибка на шаге: erase")


@router.callback_query(F.data == "erase_cancel")
async def cb_erase_cancel(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    await state.clear()
    await cb.message.answer("❌ Стирание отменено.", reply_markup=main_kb())
    await cb.answer()


# ---------------------------------------------------------------------------
# /changepass — смена пароля (FSM)
# ---------------------------------------------------------------------------

@router.message(Command("changepass"))
@router.message(F.text == "🔑 Сменить пароль")
async def cmd_changepass(m: Message, state: FSMContext):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await state.set_state(ChangePass.current)
    await m.answer(
        "🔑 <b>Смена пароля Apple ID</b>\n\n"
        "Введите <b>текущий пароль</b>:\n"
        "<i>(сообщение будет удалено)</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(ChangePass.current))
async def chpwd_current(m: Message, state: FSMContext):
    if not await guard(m):
        return
    try:
        await m.delete()
    except Exception:
        pass
    await state.update_data(current_pwd=m.text.strip())
    await state.set_state(ChangePass.new1)
    await m.answer(
        "Введите <b>новый пароль</b>:\n<i>(сообщение будет удалено)</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(ChangePass.new1))
async def chpwd_new1(m: Message, state: FSMContext):
    if not await guard(m):
        return
    pwd = m.text.strip()
    try:
        await m.delete()
    except Exception:
        pass
    err = validate_apple_password(pwd)
    if err:
        await m.answer(f"❌ {err}\nПопробуйте ещё раз:")
        return
    await state.update_data(new_pwd=pwd)
    await state.set_state(ChangePass.new2)
    await m.answer("Повторите <b>новый пароль</b>:", parse_mode="HTML")


@router.message(StateFilter(ChangePass.new2))
async def chpwd_new2(m: Message, state: FSMContext):
    if not await guard(m):
        return
    pwd2 = m.text.strip()
    try:
        await m.delete()
    except Exception:
        pass
    data = await state.get_data()
    await state.clear()
    if pwd2 != data["new_pwd"]:
        await m.answer(
            "❌ Пароли не совпадают. Начните заново: /changepass",
            reply_markup=main_kb(),
        )
        return
    await m.answer("🔄 Меняю пароль…\n<i>~2-3 минуты</i>", parse_mode="HTML")
    from apple_automation import change_password
    # password= in _pw_args() is the current password; new_password= is the new one
    r = await _run_pw(
        change_password,
        timeout=240,
        password=data["current_pwd"],
        new_password=data["new_pwd"],
    )
    if r["ok"]:
        db.set_config("password", data["new_pwd"])
        db.set_config("last_chpass", datetime.now().strftime("%d.%m.%Y %H:%M"))
        db.log_action("changepass", "", "ok")
        upd = r.get("last_updated", "")
        await m.answer(
            f"✅ <b>Пароль успешно изменён!</b>\n"
            f"{'📅 Последнее обновление: ' + upd if upd else ''}\n\n"
            "Рекомендую: /mail для проверки подтверждения",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    else:
        db.log_action("changepass", "", f"fail: {r['error']}")
        await m.answer(f"❌ Ошибка смены пароля: {r['error']}", reply_markup=main_kb())
        if r.get("screenshot"):
            await _send_screenshot(m, r["screenshot"], "Ошибка на шаге: changepass")


# ---------------------------------------------------------------------------
# FSM: смена пароля из потока NewDeviceAction
# ---------------------------------------------------------------------------

@router.message(StateFilter(NewDeviceAction.change_pass_current))
async def newdev_chpwd_current(m: Message, state: FSMContext):
    if not await guard(m):
        return
    try:
        await m.delete()
    except Exception:
        pass
    await state.update_data(current_pwd=m.text.strip())
    await state.set_state(NewDeviceAction.change_pass_new1)
    await m.answer("Введите <b>новый пароль</b>:", parse_mode="HTML")


@router.message(StateFilter(NewDeviceAction.change_pass_new1))
async def newdev_chpwd_new1(m: Message, state: FSMContext):
    if not await guard(m):
        return
    try:
        await m.delete()
    except Exception:
        pass
    err = validate_apple_password(m.text.strip())
    if err:
        await m.answer(f"❌ {err}")
        return
    await state.update_data(new_pwd=m.text.strip())
    await state.set_state(NewDeviceAction.change_pass_new2)
    await m.answer("Повторите <b>новый пароль</b>:", parse_mode="HTML")


@router.message(StateFilter(NewDeviceAction.change_pass_new2))
async def newdev_chpwd_new2(m: Message, state: FSMContext):
    if not await guard(m):
        return
    try:
        await m.delete()
    except Exception:
        pass
    data = await state.get_data()
    if m.text.strip() != data["new_pwd"]:
        await m.answer("❌ Пароли не совпадают. Начните заново.")
        return
    await state.clear()
    await m.answer("🔄 Меняю пароль…")
    from apple_automation import change_password
    r = await _run_pw(
        change_password,
        timeout=240,
        password=data["current_pwd"],
        new_password=data["new_pwd"],
    )
    if r["ok"]:
        db.set_config("password", data["new_pwd"])
        db.set_config("last_chpass", datetime.now().strftime("%d.%m.%Y %H:%M"))
        db.log_action("changepass_autoprotect", data.get("threat_device", ""), "ok")
        threat = data.get("threat_device", "")
        await m.answer(
            "✅ <b>Пароль изменён!</b>\n\n"
            + (f"Теперь стереть устройство «{threat}»?" if threat else ""),
            parse_mode="HTML",
            reply_markup=yn_kb(
                f"erase_confirm_{threat[:30]}" if threat else "erase_cancel",
                "erase_cancel",
            ),
        )
    else:
        await m.answer(f"❌ Ошибка: {r['error']}", reply_markup=main_kb())
        if r.get("screenshot"):
            await _send_screenshot(m, r["screenshot"], "Ошибка: changepass")


# ---------------------------------------------------------------------------
# /mail
# ---------------------------------------------------------------------------

@router.message(Command("mail"))
@router.message(F.text == "📬 Почта")
async def cmd_mail(m: Message):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await m.answer("🔄 Проверяю почту iCloud…\n<i>~2-3 минуты</i>", parse_mode="HTML")
    from apple_automation import check_mail
    r = await _run_pw(check_mail, timeout=240)
    if not r["ok"]:
        await m.answer(f"❌ Ошибка: {r['error']}", reply_markup=main_kb())
        if r.get("screenshot"):
            await _send_screenshot(m, r["screenshot"], "Ошибка на шаге: mail")
        return
    mails  = r["mails"]
    unread = r["unread"]
    if not mails:
        await m.answer("📬 Писем не найдено.", reply_markup=main_kb())
        return
    lines = [f"📬 <b>Почта iCloud</b> (непрочитанных: {unread})\n"]
    for i, mail in enumerate(mails[:10], 1):
        prefix = "🚨" if mail.get("is_apple") else "📧"
        unread_mark = "🔵 " if mail.get("unread") else ""
        lines.append(
            f"{prefix} <b>{i}. {unread_mark}{mail.get('subject', '—')}</b>\n"
            f"   От: {mail.get('sender', '—')}\n"
            f"   {mail.get('date', '')}"
        )
    db.log_action("mail", f"count={len(mails)},unread={unread}", "ok")
    await m.answer("\n\n".join(lines), parse_mode="HTML", reply_markup=main_kb())


# ---------------------------------------------------------------------------
# /security
# ---------------------------------------------------------------------------

@router.message(Command("security"))
@router.message(F.text == "🔒 Безопасность")
async def cmd_security(m: Message):
    if not await guard(m):
        return
    if not db.is_setup_complete():
        await m.answer("⚠️ Сначала выполните /setup")
        return
    await m.answer("🔄 Загружаю настройки безопасности…", parse_mode="HTML")
    from apple_automation import get_security_info
    r = await _run_pw(get_security_info, timeout=180)
    if not r["ok"]:
        await m.answer(f"❌ Ошибка: {r['error']}", reply_markup=main_kb())
        return
    info = truncate(r.get("info", ""), 3500)
    await m.answer(
        f"🔒 <b>Безопасность аккаунта:</b>\n\n<pre>{info}</pre>",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


# ---------------------------------------------------------------------------
# /autoprotect on|off
# ---------------------------------------------------------------------------

@router.message(Command("autoprotect"))
async def cmd_autoprotect(m: Message):
    if not await guard(m):
        return
    parts = m.text.strip().split()
    if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
        cur = db.get_config("autoprotect", "off")
        await m.answer(
            f"Использование: /autoprotect on|off\n"
            f"Текущий статус: {'✅ вкл' if cur == 'on' else '⏸ выкл'}"
        )
        return
    val = parts[1].lower()
    db.set_config("autoprotect", val)
    if val == "on":
        await m.answer(
            "🛡 <b>Автозащита включена!</b>\n\n"
            "При обнаружении нового устройства бот автоматически:\n"
            "1. Уведомит вас с деталями устройства\n"
            "2. Предложит сменить пароль\n"
            "3. Предложит стереть устройство\n"
            "4. Проверит почту на подтверждение",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    else:
        await m.answer("⏸ Автозащита выключена.", reply_markup=main_kb())


# ---------------------------------------------------------------------------
# /monitor start|stop — фоновый мониторинг
# ---------------------------------------------------------------------------

@router.message(Command("monitor"))
async def cmd_monitor(m: Message):
    if not await guard(m):
        return
    global _monitor_task
    parts = m.text.strip().split()
    if len(parts) < 2 or parts[1].lower() not in ("start", "stop"):
        cur = db.get_config("monitor", "off")
        active = _monitor_task and not _monitor_task.done()
        await m.answer(
            f"Использование: /monitor start|stop\n"
            f"Статус: {'✅ активен' if active else '⏸ остановлен'}\n"
            f"Интервал: {MONITOR_INTERVAL} сек"
        )
        return
    action = parts[1].lower()
    if action == "start":
        if _monitor_task and not _monitor_task.done():
            await m.answer("🔍 Мониторинг уже запущен.")
            return
        if not db.is_setup_complete():
            await m.answer("⚠️ Сначала выполните /setup")
            return
        db.set_config("monitor", "on")
        _monitor_task = asyncio.create_task(_monitor_loop())
        await m.answer(
            "✅ <b>Мониторинг запущен!</b>\n\n"
            f"⏱ Проверка каждые {MONITOR_INTERVAL} сек ({MONITOR_INTERVAL // 60} мин)\n"
            "📍 Источник: Find My\n"
            "🚨 При новом устройстве — немедленный алерт\n\n"
            "Остановить: /monitor stop",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
        logger.info("[monitor] Запущен пользователем")
    else:
        db.set_config("monitor", "off")
        if _monitor_task and not _monitor_task.done():
            _monitor_task.cancel()
            _monitor_task = None
        await m.answer("⏸ <b>Мониторинг остановлен.</b>", parse_mode="HTML", reply_markup=main_kb())
        logger.info("[monitor] Остановлен пользователем")


# ---------------------------------------------------------------------------
# Фоновый цикл мониторинга
# ---------------------------------------------------------------------------

async def _monitor_loop() -> None:
    """
    Запускается как asyncio.Task.
    Каждые MONITOR_INTERVAL секунд проверяет Find My на новые устройства.
    При обнаружении нового устройства:
      1. Отправляет алерт с деталями
      2. Если autoprotect=on — автоматически запрашивает детали с /devices
    """
    logger.info(f"[monitor] Цикл запущен, интервал {MONITOR_INTERVAL} сек")
    while True:
        try:
            await asyncio.sleep(MONITOR_INTERVAL)

            if db.get_config("monitor", "off") != "on":
                logger.info("[monitor] Флаг выключен — останавливаюсь")
                break

            if not db.is_setup_complete():
                logger.warning("[monitor] Setup не завершён — пропускаю итерацию")
                continue

            logger.info("[monitor] Итерация: проверка Find My...")
            known = db.get_known_devices()
            from apple_automation import monitor_check
            r = await asyncio.wait_for(
                monitor_check(known_devices=known, **_pw_args()),
                timeout=300,
            )

            if not r["ok"]:
                logger.warning(f"[monitor] Ошибка: {r['error']}")
                await notify(f"⚠️ <b>Мониторинг: ошибка проверки</b>\n{r['error']}")
                await asyncio.sleep(60)
                continue

            # Сохраняем все текущие устройства как известные
            for d in r["devices"]:
                name = d.get("name", "")
                if name:
                    db.save_known_device(name, d.get("model", ""), d.get("imei", ""))

            if r["new_devices"]:
                logger.info(f"[monitor] 🚨 Новых устройств: {len(r['new_devices'])}")
                await _alert_new_devices(OWNER_TELEGRAM_ID, r["new_devices"])

                # Если autoprotect включён — сразу получаем полные детали с IMEI
                if db.get_config("autoprotect", "off") == "on":
                    logger.info("[monitor] autoprotect=on — получаю детали устройств...")
                    from apple_automation import get_devices
                    dr = await asyncio.wait_for(
                        get_devices(**_pw_args()), timeout=300
                    )
                    if dr["ok"] and dr["devices"]:
                        text = (
                            "📱 <b>Полные детали устройств (account/manage/devices):</b>\n\n"
                            + _fmt_devices(dr["devices"])
                        )
                        await notify(text)
                        # Обновляем IMEI в известных устройствах
                        for d in dr["devices"]:
                            name = d.get("name", "")
                            if name:
                                db.save_known_device(
                                    name, d.get("model", ""), d.get("imei", "")
                                )
            else:
                logger.info(f"[monitor] Новых устройств нет. Всего: {len(r['devices'])}")

        except asyncio.CancelledError:
            logger.info("[monitor] Задача отменена")
            break
        except asyncio.TimeoutError:
            logger.warning("[monitor] Таймаут итерации (300 сек)")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"[monitor] Неожиданная ошибка: {e}")
            await notify(f"⚠️ <b>Мониторинг: критическая ошибка</b>\n{e}")
            await asyncio.sleep(120)


# ---------------------------------------------------------------------------
# ⚙️ Настройки (кнопка меню)
# ---------------------------------------------------------------------------

@router.message(F.text == "⚙️ Настройки")
async def btn_settings(m: Message):
    if not await guard(m):
        return
    cfg = _get_cfg()
    mon    = cfg.get("monitor", "off")
    ap     = cfg.get("autoprotect", "off")
    active = _monitor_task and not _monitor_task.done()
    known  = len(db.get_known_devices())
    await m.answer(
        "⚙️ <b>Настройки</b>\n\n"
        f"📧 Email: <code>{cfg.get('email', '—')}</code>\n"
        f"🔍 Мониторинг: {'✅ вкл' if active else '⏸ выкл'}\n"
        f"🛡 Автозащита: {'✅ вкл' if ap == 'on' else '⏸ выкл'}\n"
        f"📱 Известных устройств: {known}\n"
        f"⏱ Интервал: {MONITOR_INTERVAL} сек\n\n"
        "<b>Команды:</b>\n"
        "/setup — изменить настройки\n"
        "/monitor start|stop\n"
        "/autoprotect on|off\n"
        "/status — полный статус",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Мониторинг вкл" if not active else "⏸ Мониторинг выкл",
                    callback_data="mon_toggle",
                ),
                InlineKeyboardButton(
                    text="🛡 Автозащита вкл" if ap == "off" else "🛡 Автозащита выкл",
                    callback_data="ap_toggle",
                ),
            ],
            [InlineKeyboardButton(text="🔄 Изменить настройки", callback_data="go_setup")],
        ]),
    )


@router.callback_query(F.data == "mon_toggle")
async def cb_mon_toggle(cb: CallbackQuery):
    if not await guard(cb):
        return
    global _monitor_task
    active = _monitor_task and not _monitor_task.done()
    if not active:
        if not db.is_setup_complete():
            await cb.answer("Сначала выполните /setup", show_alert=True)
            return
        db.set_config("monitor", "on")
        _monitor_task = asyncio.create_task(_monitor_loop())
        await cb.answer("✅ Мониторинг запущен")
        await cb.message.answer(f"✅ Мониторинг запущен (каждые {MONITOR_INTERVAL} сек)")
    else:
        db.set_config("monitor", "off")
        if _monitor_task and not _monitor_task.done():
            _monitor_task.cancel()
        await cb.answer("⏸ Мониторинг остановлен")
        await cb.message.answer("⏸ Мониторинг остановлен")


@router.callback_query(F.data == "ap_toggle")
async def cb_ap_toggle(cb: CallbackQuery):
    if not await guard(cb):
        return
    cur = db.get_config("autoprotect", "off")
    new = "on" if cur == "off" else "off"
    db.set_config("autoprotect", new)
    await cb.answer(f"Автозащита {'включена' if new == 'on' else 'выключена'}")
    await cb.message.answer(
        f"🛡 Автозащита {'✅ включена' if new == 'on' else '⏸ выключена'}"
    )


@router.callback_query(F.data == "go_setup")
async def cb_go_setup(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return
    await cb.answer()
    await state.set_state(Setup.email)
    await cb.message.answer(
        "⚙️ Шаг 1/6: Введите ваш Apple ID (email):",
        reply_markup=ReplyKeyboardRemove(),
    )


# ---------------------------------------------------------------------------
# Фабрика бота и диспетчера
# ---------------------------------------------------------------------------

def create_bot_and_dispatcher() -> tuple[Bot, Dispatcher]:
    global _bot_instance
    bot = Bot(token=TELEGRAM_TOKEN)
    _bot_instance = bot
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return bot, dp
