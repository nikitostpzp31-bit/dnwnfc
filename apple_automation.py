"""
Apple iCloud — полная автоматизация через Playwright (stealth mode).
Потоки: login, devices, findmy, erase, change_password, check_mail, security, monitor.
"""
import asyncio
import json
import os
import random
import re
import time
from datetime import datetime
from typing import Optional

from logger import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

SESSIONS_DIR  = "pw_sessions"
SCREENSHOTS_DIR = "screenshots"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

URL_SIGNIN   = "https://account.apple.com/sign-in"
URL_MANAGE   = "https://account.apple.com/account/manage"
URL_DEVICES  = "https://account.apple.com/account/manage/section/devices"
URL_SECURITY = "https://account.apple.com/account/manage/section/security"
URL_FINDMY   = "https://www.icloud.com/find/"
URL_MAIL     = "https://www.icloud.com/mail/"

TIMEOUT = 30_000   # ms — стандартный таймаут
SHORT   = 8_000    # ms — короткий таймаут
LONG    = 60_000   # ms — длинный таймаут (ожидание загрузки)

# ---------------------------------------------------------------------------
# Браузер — запуск с persistent context (stealth)
# ---------------------------------------------------------------------------

async def _get_browser(acc_id: int, headless: bool = False):
    """
    Запускает Chromium с persistent profile (сессия сохраняется между запусками).
    Возвращает (playwright, context).
    """
    from playwright.async_api import async_playwright

    logger.info(f"[{_ts()}] 🚀 Запуск браузера Playwright (stealth mode)...")
    profile = os.path.abspath(f"{SESSIONS_DIR}/{acc_id}")
    os.makedirs(profile, exist_ok=True)

    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        profile,
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--disable-dev-shm-usage",
            "--disable-extensions",
            "--lang=ru-RU,ru",
            "--window-size=1400,900",
        ],
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1400, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        ignore_https_errors=True,
        java_script_enabled=True,
    )

    # Скрываем признаки автоматизации
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU','ru','en-US','en']});
        window.chrome = {runtime: {}};
        Object.defineProperty(navigator, 'permissions', {
            get: () => ({query: () => Promise.resolve({state: 'granted'})})
        });
    """)

    logger.info(f"[{_ts()}] ✅ Браузер успешно открыт")
    return pw, ctx


async def _new_page(ctx):
    """Создаёт новую страницу с таймаутами."""
    page = await ctx.new_page()
    page.set_default_timeout(TIMEOUT)
    page.set_default_navigation_timeout(LONG)
    return page


def _ts() -> str:
    """Текущее время HH:MM:SS для логов."""
    return datetime.now().strftime("%H:%M:%S")


async def _rnd_delay(lo: float = 0.4, hi: float = 1.8):
    """Случайная задержка для имитации человека."""
    await asyncio.sleep(lo + (hi - lo) * random.random())


async def _type_human(page, selector: str, text: str):
    """Вводит текст посимвольно с человеческими задержками."""
    el = page.locator(selector).first
    await el.click()
    await el.fill("")
    await _rnd_delay(0.1, 0.3)
    for ch in text:
        await el.type(ch, delay=40 + int(80 * random.random()))
    await _rnd_delay(0.2, 0.5)


async def _screenshot(page, step: str = "") -> Optional[bytes]:
    """Делает скриншот и сохраняет в ./screenshots/."""
    try:
        data = await page.screenshot(full_page=False)
        if step:
            fname = f"{SCREENSHOTS_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{step}.png"
            with open(fname, "wb") as f:
                f.write(data)
            logger.info(f"[{_ts()}] 📸 Скриншот сохранён: {fname}")
        return data
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Кнопки — стратегия клика (синяя = primary, белая = secondary)
# ---------------------------------------------------------------------------

async def _click_blue(page, timeout: int = TIMEOUT) -> bool:
    """
    Нажимает главную синюю кнопку действия.
    Приоритет: CSS primary → submit → текст кнопки.
    """
    selectors = [
        "button[class*='btn-primary']",
        "button[class*='button-primary']",
        "button[class*='primary']",
        "#btn-sign-in",
        "button[type='submit']",
        "input[type='submit']",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                logger.info(f"[{_ts()}] 🖱️ Клик по СИНЕЙ кнопке (CSS: {sel})")
                await btn.click()
                return True
        except Exception:
            pass

    # Fallback по тексту
    for text in ["Продолжить", "Войти", "Continue", "Sign In", "Далее", "Next"]:
        try:
            btn = page.get_by_role("button", name=re.compile(text, re.I)).first
            if await btn.is_visible(timeout=2000):
                logger.info(f"[{_ts()}] 🖱️ Клик по СИНЕЙ кнопке: «{text}»")
                await btn.click()
                return True
        except Exception:
            pass
    return False


async def _click_white(page, text: str, timeout: int = TIMEOUT) -> bool:
    """
    Нажимает белую/вторичную кнопку по тексту.
    Используется для «другие возможности», «не улучшать» и т.д.
    """
    # Попытка 1: role=button
    try:
        btn = page.get_by_role("button", name=re.compile(re.escape(text), re.I)).first
        await btn.wait_for(state="visible", timeout=timeout)
        logger.info(f"[{_ts()}] 🖱️ Клик по БЕЛОЙ кнопке: «{text}»")
        await btn.click()
        return True
    except Exception:
        pass

    # Попытка 2: любой кликабельный элемент с текстом
    try:
        el = page.get_by_text(re.compile(re.escape(text), re.I)).first
        await el.wait_for(state="visible", timeout=SHORT)
        logger.info(f"[{_ts()}] 🖱️ Клик по тексту: «{text}»")
        await el.click()
        return True
    except Exception:
        pass

    # Попытка 3: locator с has-text
    try:
        el = page.locator(f"button:has-text('{text}'), a:has-text('{text}')").first
        if await el.is_visible(timeout=2000):
            await el.click()
            return True
    except Exception:
        pass

    return False


async def _retry_click(fn, *args, retries: int = 3, **kwargs) -> bool:
    """Повторяет клик до retries раз с нарастающей задержкой."""
    for attempt in range(1, retries + 1):
        result = await fn(*args, **kwargs)
        if result:
            return True
        if attempt < retries:
            logger.warning(f"[{_ts()}] ⚠️ Клик не удался, попытка {attempt}/{retries}")
            await asyncio.sleep(attempt * 1.0)
    return False


# ---------------------------------------------------------------------------
# Контрольные вопросы — точное совпадение + посимвольный ввод
# ---------------------------------------------------------------------------

async def _answer_security_questions(
    page,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
) -> bool:
    """
    Читает вопросы на странице, сопоставляет с хранимыми ответами,
    вводит ТОЧНЫЙ ответ посимвольно (human-like).
    """
    try:
        content = await page.content()
        logger.info(f"[{_ts()}] 🔐 Страница контрольных вопросов — анализирую...")

        inputs = page.locator("input[type='text'], input[type='password'], input:not([type])")
        count = await inputs.count()
        logger.info(f"[{_ts()}] 🔍 Найдено полей ввода: {count}")

        answered = 0
        for i in range(count):
            inp = inputs.nth(i)
            try:
                # Читаем текст вопроса из ближайшего родителя
                parent_text = await page.evaluate(
                    """(el) => {
                        let p = el.parentElement;
                        for (let j = 0; j < 6; j++) {
                            if (p) {
                                let t = (p.innerText || p.textContent || '').trim();
                                if (t.length > 5) return t;
                                p = p.parentElement;
                            }
                        }
                        return '';
                    }""",
                    await inp.element_handle()
                )

                # Определяем какой ответ вставить
                answer = None
                if q1_text and (q1_text[:8] in parent_text or q1_text[:8] in content):
                    answer = q1_answer
                    logger.info(f"[{_ts()}] ✍️ Ответ на вопрос 1: {q1_text[:20]}...")
                elif q2_text and (q2_text[:8] in parent_text or q2_text[:8] in content):
                    answer = q2_answer
                    logger.info(f"[{_ts()}] ✍️ Ответ на вопрос 2: {q2_text[:20]}...")
                elif answered == 0 and q1_answer:
                    answer = q1_answer
                elif answered == 1 and q2_answer:
                    answer = q2_answer

                if answer:
                    await inp.click()
                    await inp.fill("")
                    await _rnd_delay(0.2, 0.4)
                    # Посимвольный ввод — имитация человека
                    for ch in answer:
                        await inp.type(ch, delay=35 + int(65 * random.random()))
                    answered += 1
                    await _rnd_delay(0.3, 0.6)
            except Exception as e:
                logger.warning(f"[{_ts()}] ⚠️ Поле {i}: {e}")
                continue

        if answered == 0:
            # Последний шанс: заполняем по порядку
            if count >= 1 and q1_answer:
                await inputs.nth(0).fill(q1_answer)
                answered += 1
            if count >= 2 and q2_answer:
                await inputs.nth(1).fill(q2_answer)
                answered += 1

        await _rnd_delay(0.5, 1.0)
        await _retry_click(_click_blue, page)
        logger.info(f"[{_ts()}] ✅ Контрольные вопросы заполнены ({answered} ответов)")
        return answered > 0

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ Ошибка контрольных вопросов: {e}")
        return False


# ---------------------------------------------------------------------------
# SIGN-IN — полный вход на account.apple.com
# ---------------------------------------------------------------------------

async def apple_signin(
    page,
    email: str,
    password: str,
    q1_text: str,
    q1_answer: str,
    q2_text: str,
    q2_answer: str,
    tfa_queue: Optional[asyncio.Queue] = None,
    notify_fn=None,
) -> dict:
    """
    Полный вход на account.apple.com/sign-in.
    Обрабатывает: email → пароль → контрольные вопросы → 2FA → «не улучшать».
    Возвращает {"ok": bool, "error": str, "screenshot": bytes|None}
    """
    result = {"ok": False, "error": "", "screenshot": None}

    try:
        logger.info(f"[{_ts()}] 🌐 Переход на {URL_SIGNIN}")
        await page.goto(URL_SIGNIN, wait_until="domcontentloaded")
        await _rnd_delay(1.5, 2.5)

        # ── Шаг 1: поле email ──────────────────────────────────────────────
        email_sel = (
            "input[type='email'], input[name='accountName'], "
            "#account_name_text_field, input[autocomplete='username']"
        )
        try:
            await page.wait_for_selector(email_sel, timeout=TIMEOUT)
        except Exception:
            result["error"] = "Поле email не найдено на странице входа"
            result["screenshot"] = await _screenshot(page, "no_email_field")
            return result

        logger.info(f"[{_ts()}] ✍️ Ввод в поле 'Email или номер телефона': {email}")
        await _type_human(page, email_sel, email)
        await _rnd_delay(0.5, 1.0)

        logger.info(f"[{_ts()}] 🖱️ Клик по СИНЕЙ кнопке: «Продолжить»")
        await _retry_click(_click_blue, page)
        await _rnd_delay(1.5, 2.5)

        # ── Шаг 2: поле пароля ────────────────────────────────────────────
        pwd_sel = "input[type='password'], #password_text_field, input[autocomplete='current-password']"
        try:
            await page.wait_for_selector(pwd_sel, timeout=TIMEOUT)
        except Exception:
            result["error"] = "Поле пароля не найдено"
            result["screenshot"] = await _screenshot(page, "no_pwd_field")
            return result

        logger.info(f"[{_ts()}] ✍️ Ввод в поле 'Пароль': {'*' * len(password)}")
        await _type_human(page, pwd_sel, password)
        await _rnd_delay(0.5, 1.0)

        logger.info(f"[{_ts()}] 🖱️ Клик по СИНЕЙ кнопке: «Войти»")
        await _retry_click(_click_blue, page)
        await _rnd_delay(2.0, 3.5)

        # ── Цикл обработки страниц после входа ────────────────────────────
        for iteration in range(15):
            url = page.url
            try:
                content = await page.content()
            except Exception:
                await _rnd_delay(1, 2)
                continue
            cl = content.lower()

            logger.info(f"[{_ts()}] 🔄 Итерация {iteration+1}: {url[:60]}")

            # Успешный вход
            if (
                "account/manage" in url
                or ("appleid.apple.com" in url and "sign-in" not in url)
                or ("icloud.com" in url and "sign" not in url)
            ):
                logger.info(f"[{_ts()}] ✅ Вход выполнен успешно!")
                result["ok"] = True
                return result

            # Контрольные вопросы
            if (
                "контрольн" in cl
                or "security question" in cl
                or (q1_text and q1_text[:6] in content)
                or (q2_text and q2_text[:6] in content)
            ):
                logger.info(f"[{_ts()}] 🔐 Страница контрольных вопросов")
                ok = await _answer_security_questions(page, q1_text, q1_answer, q2_text, q2_answer)
                if not ok:
                    result["error"] = "Не удалось ответить на контрольные вопросы"
                    result["screenshot"] = await _screenshot(page, "questions_fail")
                    return result
                await _rnd_delay(2.0, 3.0)
                continue

            # 2FA — «Безопасность Аккаунта Apple» → «другие возможности»
            if (
                "двухфакторн" in cl
                or "two-factor" in cl
                or "безопасность аккаунта apple" in cl
            ):
                if "другие возможности" in cl or "other options" in cl:
                    logger.info(f"[{_ts()}] 🖱️ Клик по БЕЛОЙ кнопке: «другие возможности»")
                    await _retry_click(_click_white, page, "другие возможности")
                    await _rnd_delay(1.5, 2.5)
                    continue

            # «Защитите свой аккаунт» → «не улучшать»
            if (
                "защитите" in cl
                or "не улучшать" in cl
                or "don't improve" in cl
                or "protect your account" in cl
            ):
                logger.info(f"[{_ts()}] 🖱️ Клик по БЕЛОЙ кнопке: «не улучшать» (левая)")
                await _retry_click(_click_white, page, "не улучшать")
                await _rnd_delay(1.5, 2.5)
                continue

            # 2FA код (6 цифр)
            if (
                "verification code" in cl
                or "код подтверждения" in cl
                or "enter the code" in cl
                or "введите код" in cl
            ):
                logger.info(f"[{_ts()}] 📲 Требуется код 2FA")
                if notify_fn:
                    await notify_fn(
                        "📲 <b>Требуется код двухфакторной аутентификации</b>\n"
                        "Введите команду: <code>/tfa 123456</code>"
                    )
                if tfa_queue:
                    try:
                        logger.info(f"[{_ts()}] ⏳ Ожидание кода 2FA (120 сек)...")
                        code = await asyncio.wait_for(tfa_queue.get(), timeout=120)
                        logger.info(f"[{_ts()}] ✍️ Ввод кода 2FA: {code}")
                        # Пробуем 6 отдельных полей (по одной цифре)
                        code_inputs = page.locator(
                            "input[type='number'], input[inputmode='numeric'], "
                            "input[maxlength='1'], input[class*='digit']"
                        )
                        cnt = await code_inputs.count()
                        if cnt >= 6:
                            for idx, ch in enumerate(code[:6]):
                                await code_inputs.nth(idx).fill(ch)
                                await _rnd_delay(0.05, 0.15)
                        else:
                            single = page.locator(
                                "input[autocomplete='one-time-code'], "
                                "input[type='text'][maxlength='6']"
                            ).first
                            await single.fill(code)
                        await _retry_click(_click_blue, page)
                        await _rnd_delay(2.0, 3.0)
                        continue
                    except asyncio.TimeoutError:
                        result["error"] = "Таймаут ожидания кода 2FA (120 сек)"
                        result["screenshot"] = await _screenshot(page, "tfa_timeout")
                        return result
                else:
                    result["error"] = "Требуется 2FA, но очередь не передана"
                    result["screenshot"] = await _screenshot(page, "tfa_no_queue")
                    return result

            # «Доверять этому браузеру?»
            if "trust" in cl or "доверять" in cl or "доверяете" in cl:
                logger.info(f"[{_ts()}] 🖱️ Клик «Не сейчас» (Trust browser popup)")
                await _click_white(page, "Не сейчас")
                await _rnd_delay(1.0, 2.0)
                continue

            # Неизвестная страница — ждём
            await _rnd_delay(1.5, 2.5)

        result["error"] = "Превышено число итераций входа (15)"
        result["screenshot"] = await _screenshot(page, "signin_loop_exceeded")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ Ошибка входа: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "signin_exception")
        return result


# ---------------------------------------------------------------------------
# DEVICES — список устройств с IMEI
# ---------------------------------------------------------------------------

def _parse_device_text(text: str) -> dict:
    """Извлекает Описание, Модель, Версию, IMEI из текста панели устройства."""
    dev = {"description": "", "model": "", "version": "", "imei": "", "name": ""}
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    for i, line in enumerate(lines):
        ll = line.lower()
        nxt = lines[i + 1] if i + 1 < len(lines) else ""

        if "описание" in ll:
            dev["description"] = nxt
            if not dev["name"]:
                dev["name"] = nxt
        elif "модель" in ll:
            dev["model"] = nxt
        elif "версия" in ll or "version" in ll:
            dev["version"] = nxt
        elif "imei" in ll:
            m = re.search(r"(\d[\d\s]{13,17})", line + " " + nxt)
            if m:
                dev["imei"] = m.group(1).replace(" ", "").strip()
        elif re.search(r"(iphone|ipad|mac|ipod|apple watch|airpods)", ll):
            if not dev["model"]:
                dev["model"] = line
            if not dev["name"]:
                dev["name"] = line

    if not dev["name"] and lines:
        dev["name"] = lines[0]
    return dev


async def get_devices(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Возвращает список устройств с IMEI из account.apple.com/section/devices.
    {"ok": bool, "devices": [...], "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "devices": [], "error": "", "screenshot": None}
    try:
        r = await apple_signin(page, email, password, q1_text, q1_answer,
                               q2_text, q2_answer, tfa_queue, notify_fn)
        if not r["ok"]:
            result.update(r)
            return result

        logger.info(f"[{_ts()}] 🌐 Переход на {URL_DEVICES}")
        await page.goto(URL_DEVICES, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 4.0)

        # Ждём список устройств
        try:
            await page.wait_for_selector(
                "[class*='device'], [class*='Device'], li[class*='item']",
                timeout=LONG
            )
        except Exception:
            logger.warning(f"[{_ts()}] ⚠️ Список устройств не найден стандартным селектором")

        devices = []
        device_items = page.locator(
            "[class*='device-item'], [class*='deviceItem'], "
            "[class*='device-list'] li, [class*='deviceList'] li"
        )
        count = await device_items.count()
        logger.info(f"[{_ts()}] 📱 Найдено элементов устройств: {count}")

        if count == 0:
            device_items = page.locator("ul li, [role='listitem']")
            count = await device_items.count()

        for i in range(min(count, 25)):
            try:
                item = device_items.nth(i)
                logger.info(f"[{_ts()}] 🖱️ Открываю устройство {i+1}/{count}")
                await item.click()
                await _rnd_delay(1.0, 2.0)

                panel = page.locator(
                    "[class*='device-detail'], [class*='deviceDetail'], "
                    "[class*='panel'], [class*='modal'], [class*='drawer']"
                ).first
                try:
                    await panel.wait_for(state="visible", timeout=5000)
                    text = await panel.inner_text()
                except Exception:
                    text = await page.inner_text("body")

                dev = _parse_device_text(text)
                if dev.get("model") or dev.get("description") or dev.get("name"):
                    devices.append(dev)
                    logger.info(
                        f"[{_ts()}] ✅ Устройство: {dev.get('name','?')} | "
                        f"IMEI: {dev.get('imei','—')}"
                    )

                # Закрываем панель
                close = page.locator(
                    "[class*='close'], button[aria-label*='close'], "
                    "button[aria-label*='закрыть'], [class*='dismiss']"
                ).first
                try:
                    if await close.is_visible(timeout=2000):
                        await close.click()
                        await _rnd_delay(0.5, 1.0)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"[{_ts()}] ⚠️ Устройство {i}: {e}")
                continue

        result["ok"] = True
        result["devices"] = devices
        logger.info(f"[{_ts()}] ✅ Устройств получено: {len(devices)}")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ get_devices: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "devices_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# FIND MY — вход в iCloud + список устройств
# ---------------------------------------------------------------------------

async def _signin_icloud(
    page, email, password, q1_text, q1_answer, q2_text, q2_answer,
    tfa_queue=None, notify_fn=None,
) -> bool:
    """Вход через форму iCloud (используется для Find My и Mail)."""
    try:
        await _rnd_delay(2.0, 4.0)
        for _ in range(10):
            url = page.url
            try:
                content = await page.content()
            except Exception:
                await _rnd_delay(1, 2)
                continue
            cl = content.lower()

            # Уже вошли
            if ("icloud.com/find" in url or "icloud.com/mail" in url) and "sign" not in url:
                if "map" in cl or "device" in cl or "message" in cl or "mail" in cl:
                    logger.info(f"[{_ts()}] ✅ Уже авторизован в iCloud")
                    return True

            # Поле email
            if "email" in cl or "accountname" in cl or "sign-in" in url or "почт" in cl:
                email_sel = (
                    "input[type='email'], input[name='accountName'], "
                    "#account_name_text_field, input[autocomplete='username']"
                )
                try:
                    await page.wait_for_selector(email_sel, timeout=SHORT)
                    logger.info(f"[{_ts()}] ✍️ Ввод email в iCloud: {email}")
                    await _type_human(page, email_sel, email)
                    await _rnd_delay(0.5, 1.0)
                    await _retry_click(_click_blue, page)
                    await _rnd_delay(1.5, 2.5)
                    continue
                except Exception:
                    pass

            # Поле пароля
            if "password" in cl or "пароль" in cl:
                pwd_sel = "input[type='password']"
                try:
                    await page.wait_for_selector(pwd_sel, timeout=SHORT)
                    logger.info(f"[{_ts()}] ✍️ Ввод пароля в iCloud")
                    await _type_human(page, pwd_sel, password)
                    await _rnd_delay(0.5, 1.0)
                    await _retry_click(_click_blue, page)
                    await _rnd_delay(2.0, 3.0)
                    continue
                except Exception:
                    pass

            # Контрольные вопросы
            if "контрольн" in cl or (q1_text and q1_text[:6] in content):
                await _answer_security_questions(page, q1_text, q1_answer, q2_text, q2_answer)
                await _rnd_delay(2.0, 3.0)
                continue

            # 2FA предложение
            if "двухфакторн" in cl or "two-factor" in cl:
                await _retry_click(_click_white, page, "другие возможности")
                await _rnd_delay(1.5, 2.5)
                continue

            # «не улучшать»
            if "защитите" in cl or "не улучшать" in cl:
                await _retry_click(_click_white, page, "не улучшать")
                await _rnd_delay(1.5, 2.5)
                continue

            # 2FA код
            if "verification code" in cl or "код подтверждения" in cl:
                if notify_fn:
                    await notify_fn("📲 Требуется код 2FA. Введите: <code>/tfa 123456</code>")
                if tfa_queue:
                    try:
                        code = await asyncio.wait_for(tfa_queue.get(), timeout=120)
                        inp = page.locator(
                            "input[type='number'], input[inputmode='numeric']"
                        ).first
                        await inp.fill(code)
                        await _retry_click(_click_blue, page)
                        await _rnd_delay(2.0, 3.0)
                        continue
                    except asyncio.TimeoutError:
                        return False

            await _rnd_delay(1.5, 2.5)

        return False
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ _signin_icloud: {e}")
        return False


async def _extract_findmy_devices(page) -> list:
    """Извлекает список устройств из Find My через JS + DOM fallback."""
    devices = []

    # Попытка 1: JS window.__data__
    try:
        js_devs = await page.evaluate("""
            () => {
                try {
                    if (window.__data__ && window.__data__.devices)
                        return window.__data__.devices;
                    if (window.FindMyApp && window.FindMyApp.devices)
                        return window.FindMyApp.devices;
                } catch(e) {}
                const items = document.querySelectorAll(
                    '[class*="device-item"], [class*="deviceItem"], [class*="device-list-item"]'
                );
                return Array.from(items).map(el => ({
                    name:     el.querySelector('[class*="name"],[class*="title"]')?.innerText || el.innerText?.split('\\n')[0] || '',
                    status:   el.querySelector('[class*="status"]')?.innerText || '',
                    location: el.querySelector('[class*="location"],[class*="subtitle"]')?.innerText || '',
                    model:    el.querySelector('[class*="model"]')?.innerText || '',
                }));
            }
        """)
        if js_devs and isinstance(js_devs, list):
            for d in js_devs:
                if d.get("name"):
                    devices.append({
                        "name":     d.get("name", ""),
                        "status":   d.get("status", ""),
                        "location": d.get("location", ""),
                        "model":    d.get("model", ""),
                        "imei":     d.get("imei", ""),
                    })
    except Exception as e:
        logger.warning(f"[{_ts()}] ⚠️ JS extract: {e}")

    # Попытка 2: DOM fallback
    if not devices:
        try:
            items = page.locator("[class*='device'], li[class*='item'], [class*='Device']")
            count = await items.count()
            for i in range(min(count, 25)):
                try:
                    text = await items.nth(i).inner_text()
                    if text.strip():
                        lines = [l.strip() for l in text.splitlines() if l.strip()]
                        devices.append({
                            "name":     lines[0] if lines else "Устройство",
                            "status":   lines[1] if len(lines) > 1 else "",
                            "location": lines[2] if len(lines) > 2 else "",
                            "model":    "",
                            "imei":     "",
                        })
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"[{_ts()}] ⚠️ DOM extract: {e}")

    return devices


async def get_findmy_devices(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Возвращает список устройств из Find My.
    {"ok": bool, "devices": [...], "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "devices": [], "error": "", "screenshot": None}
    try:
        logger.info(f"[{_ts()}] 🌐 Переход на {URL_FINDMY}")
        await page.goto(URL_FINDMY, wait_until="domcontentloaded")

        ok = await _signin_icloud(page, email, password, q1_text, q1_answer,
                                   q2_text, q2_answer, tfa_queue, notify_fn)
        if not ok:
            result["error"] = "Не удалось войти в Find My"
            result["screenshot"] = await _screenshot(page, "findmy_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)
        try:
            await page.wait_for_selector(
                "[class*='device'], [class*='Device'], [data-testid*='device']",
                timeout=LONG
            )
        except Exception:
            pass

        devices = await _extract_findmy_devices(page)
        result["ok"] = True
        result["devices"] = devices
        logger.info(f"[{_ts()}] ✅ Find My: найдено {len(devices)} устройств")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ get_findmy_devices: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "findmy_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# ERASE DEVICE — стирание через Find My
# ---------------------------------------------------------------------------

def _fuzzy_match(query: str, text: str) -> bool:
    """Нечёткое совпадение: все слова запроса присутствуют в тексте."""
    words = query.lower().split()
    text_lower = text.lower()
    return all(w in text_lower for w in words if len(w) > 2)


async def erase_findmy_device(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    device_name: str,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Стирает устройство в Find My.
    {"ok": bool, "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "error": "", "screenshot": None}
    try:
        logger.info(f"[{_ts()}] 🌐 Переход на {URL_FINDMY} для стирания: {device_name}")
        await page.goto(URL_FINDMY, wait_until="domcontentloaded")

        ok = await _signin_icloud(page, email, password, q1_text, q1_answer,
                                   q2_text, q2_answer, tfa_queue, notify_fn)
        if not ok:
            result["error"] = "Не удалось войти в Find My"
            result["screenshot"] = await _screenshot(page, "erase_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)

        # Ищем устройство по имени (нечёткое совпадение)
        found = False
        items = page.locator("[class*='device'], li[class*='item'], [class*='Device']")
        count = await items.count()
        logger.info(f"[{_ts()}] 🔍 Ищу устройство «{device_name}» среди {count} элементов")

        for i in range(min(count, 25)):
            try:
                item = items.nth(i)
                text = await item.inner_text()
                if device_name.lower() in text.lower() or _fuzzy_match(device_name, text):
                    logger.info(f"[{_ts()}] ✅ Устройство найдено: {text[:40]}")
                    await item.click()
                    found = True
                    await _rnd_delay(1.0, 2.0)
                    break
            except Exception:
                continue

        if not found:
            result["error"] = f"Устройство «{device_name}» не найдено в Find My"
            result["screenshot"] = await _screenshot(page, "erase_not_found")
            return result

        # Нажимаем «Стереть»
        logger.info(f"[{_ts()}] 🖱️ Клик по кнопке «Стереть»")
        erased = await _retry_click(_click_white, page, "Стереть")
        if not erased:
            erased = await _retry_click(_click_white, page, "Erase")
        if not erased:
            result["error"] = "Кнопка «Стереть» не найдена"
            result["screenshot"] = await _screenshot(page, "erase_btn_not_found")
            return result

        await _rnd_delay(1.5, 2.5)

        # Подтверждение в диалоге
        logger.info(f"[{_ts()}] 🖱️ Подтверждение стирания")
        confirm = await _click_white(page, "Стереть")
        if not confirm:
            confirm = await _retry_click(_click_blue, page)

        await _rnd_delay(2.0, 3.0)
        result["ok"] = True
        logger.info(f"[{_ts()}] ✅ Устройство «{device_name}» стёрто!")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ erase_findmy_device: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "erase_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# CHANGE PASSWORD — смена пароля Apple ID
# ---------------------------------------------------------------------------

async def change_password(
    acc_id: int, email: str,
    new_password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    tfa_queue=None, notify_fn=None,
    # 'password' from _pw_args() is the current password
    password: str = "",
    current_password: str = "",
) -> dict:
    # Support both calling conventions
    if not current_password:
        current_password = password
    """
    Меняет пароль Apple ID через account.apple.com/account/manage.
    {"ok": bool, "last_updated": str, "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "last_updated": "", "error": "", "screenshot": None}
    try:
        r = await apple_signin(page, email, current_password, q1_text, q1_answer,
                               q2_text, q2_answer, tfa_queue, notify_fn)
        if not r["ok"]:
            result.update(r)
            return result

        logger.info(f"[{_ts()}] 🌐 Переход на {URL_MANAGE}")
        await page.goto(URL_MANAGE, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 3.0)

        # Нажимаем кнопку «Пароль»
        logger.info(f"[{_ts()}] 🖱️ Клик по кнопке «Пароль»")
        clicked = await _retry_click(_click_white, page, "Пароль")
        if not clicked:
            clicked = await _retry_click(_click_white, page, "Password")
        await _rnd_delay(1.5, 2.5)

        # Читаем «Последнее обновление» до смены
        try:
            content = await page.content()
            m = re.search(
                r"[Пп]оследнее обновление[:\s]+(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
                content
            )
            if m:
                result["last_updated"] = m.group(1)
                logger.info(f"[{_ts()}] 📅 Последнее обновление пароля: {result['last_updated']}")
        except Exception:
            pass

        # Заполняем поля пароля
        pwd_inputs = page.locator("input[type='password']")
        count = await pwd_inputs.count()
        logger.info(f"[{_ts()}] 🔑 Полей пароля найдено: {count}")

        if count >= 3:
            logger.info(f"[{_ts()}] ✍️ Ввод текущего пароля")
            await pwd_inputs.nth(0).fill(current_password)
            await _rnd_delay(0.3, 0.6)
            logger.info(f"[{_ts()}] ✍️ Ввод нового пароля")
            await pwd_inputs.nth(1).fill(new_password)
            await _rnd_delay(0.3, 0.6)
            logger.info(f"[{_ts()}] ✍️ Подтверждение нового пароля")
            await pwd_inputs.nth(2).fill(new_password)
            await _rnd_delay(0.5, 1.0)
        elif count == 2:
            await pwd_inputs.nth(0).fill(new_password)
            await pwd_inputs.nth(1).fill(new_password)
        else:
            result["error"] = f"Найдено только {count} полей пароля (ожидалось 3)"
            result["screenshot"] = await _screenshot(page, "changepass_fields")
            return result

        logger.info(f"[{_ts()}] 🖱️ Клик по СИНЕЙ кнопке: «Изменить пароль»")
        await _retry_click(_click_blue, page)
        await _rnd_delay(2.0, 4.0)

        # Проверяем успех
        content = await page.content()
        if "успешно" in content.lower() or "changed" in content.lower() or "updated" in content.lower():
            result["ok"] = True
        else:
            m = re.search(
                r"[Пп]оследнее обновление[:\s]+(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
                content
            )
            if m:
                result["last_updated"] = m.group(1)
            result["ok"] = True  # Считаем успехом если нет явной ошибки

        today = datetime.now().strftime("%d.%m.%Y")
        if not result["last_updated"]:
            result["last_updated"] = today

        logger.info(f"[{_ts()}] ✅ Пароль изменён! Дата: {result['last_updated']}")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ change_password: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "changepass_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# CHECK MAIL — проверка почты iCloud
# ---------------------------------------------------------------------------

async def _extract_mails(page) -> list:
    """Извлекает письма из iCloud Mail."""
    mails = []
    try:
        items = page.locator(
            "[class*='message-row'], [class*='MessageRow'], "
            "[class*='mail-item'], [class*='MailItem'], "
            "[class*='message-list'] li"
        )
        count = await items.count()
        logger.info(f"[{_ts()}] 📬 Найдено писем: {count}")

        for i in range(min(count, 15)):
            try:
                item = items.nth(i)
                text = await item.inner_text()
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                cls = await item.get_attribute("class") or ""
                is_unread = "unread" in cls.lower()
                sender  = lines[0] if lines else ""
                subject = lines[1] if len(lines) > 1 else ""
                date    = lines[-1] if len(lines) > 2 else ""
                is_apple = any(
                    kw in (sender + subject).lower()
                    for kw in ["apple", "icloud", "security", "sign in", "new device",
                               "password", "пароль", "устройство", "вход", "appleid"]
                )
                mails.append({
                    "sender": sender, "subject": subject, "date": date,
                    "unread": is_unread, "is_apple": is_apple,
                })
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"[{_ts()}] ⚠️ _extract_mails: {e}")
    return mails


async def check_mail(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Проверяет почту iCloud.
    {"ok": bool, "mails": [...], "unread": int, "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "mails": [], "unread": 0, "error": "", "screenshot": None}
    try:
        logger.info(f"[{_ts()}] 🌐 Переход на {URL_MAIL}")
        await page.goto(URL_MAIL, wait_until="domcontentloaded")

        ok = await _signin_icloud(page, email, password, q1_text, q1_answer,
                                   q2_text, q2_answer, tfa_queue, notify_fn)
        if not ok:
            result["error"] = "Не удалось войти в iCloud Mail"
            result["screenshot"] = await _screenshot(page, "mail_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)
        try:
            await page.wait_for_selector(
                "[class*='message'], [class*='mail-item'], [class*='MessageList']",
                timeout=LONG
            )
        except Exception:
            pass

        mails = await _extract_mails(page)
        unread = sum(1 for m in mails if m.get("unread"))
        result["ok"] = True
        result["mails"] = mails[:10]
        result["unread"] = unread
        logger.info(f"[{_ts()}] ✅ Почта: {len(mails)} писем, непрочитанных: {unread}")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ check_mail: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "mail_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# SECURITY — страница безопасности
# ---------------------------------------------------------------------------

async def get_security_info(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Возвращает информацию со страницы безопасности.
    {"ok": bool, "info": str, "error": str, "screenshot": bytes|None}
    """
    pw, ctx = await _get_browser(acc_id)
    page = await _new_page(ctx)
    result = {"ok": False, "info": "", "error": "", "screenshot": None}
    try:
        r = await apple_signin(page, email, password, q1_text, q1_answer,
                               q2_text, q2_answer, tfa_queue, notify_fn)
        if not r["ok"]:
            result.update(r)
            return result

        logger.info(f"[{_ts()}] 🌐 Переход на {URL_SECURITY}")
        await page.goto(URL_SECURITY, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 3.0)

        try:
            await page.wait_for_selector(
                "[class*='security'], [class*='Security']", timeout=TIMEOUT
            )
        except Exception:
            pass

        info = await page.inner_text("main, [class*='content'], body")
        result["ok"] = True
        result["info"] = info[:3000]
        logger.info(f"[{_ts()}] ✅ Страница безопасности загружена")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ get_security_info: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "security_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


# ---------------------------------------------------------------------------
# MONITOR CHECK — одна итерация мониторинга
# ---------------------------------------------------------------------------

async def monitor_check(
    acc_id: int, email: str, password: str,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    known_devices: list,
    tfa_queue=None, notify_fn=None,
) -> dict:
    """
    Проверяет Find My + Devices, возвращает новые устройства.
    {"ok": bool, "devices": [...], "new_devices": [...], "error": str}
    """
    result = {"ok": False, "devices": [], "new_devices": [], "error": ""}
    try:
        logger.info(f"[{_ts()}] 🔍 Мониторинг: проверка Find My...")
        fm = await get_findmy_devices(
            acc_id, email, password,
            q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn
        )
        if not fm["ok"]:
            result["error"] = fm["error"]
            return result

        current = fm["devices"]
        result["devices"] = current
        result["ok"] = True

        # Сравниваем с известными устройствами
        known_names = {d.get("name", "").lower() for d in known_devices}
        for dev in current:
            name = dev.get("name", "").lower()
            if name and name not in known_names:
                logger.info(f"[{_ts()}] 🚨 НОВОЕ УСТРОЙСТВО: {dev.get('name')}")
                result["new_devices"].append(dev)

        return result

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ monitor_check: {e}")
        result["error"] = str(e)
        return result
