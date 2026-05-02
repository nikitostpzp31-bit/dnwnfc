"""
Apple ID Automation - Playwright stealth mode.
Every step is screenshotted. All bot commands are implemented.
"""

import asyncio
import os
import random
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from logger import get_logger

# Determine headless mode: respect config but force True when no display exists
try:
    from config import HEADLESS as _CFG_HEADLESS
except Exception:
    _CFG_HEADLESS = True

import shutil as _shutil
_HAS_DISPLAY = bool(
    os.environ.get("DISPLAY") or
    os.environ.get("WAYLAND_DISPLAY") or
    _shutil.which("Xvfb")
)
HEADLESS: bool = _CFG_HEADLESS if _HAS_DISPLAY else True

logger = get_logger()

SESSIONS_DIR   = "pw_sessions"
SCREENSHOTS_DIR = "screenshots"
os.makedirs(SESSIONS_DIR,    exist_ok=True)
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

URL_SIGNIN   = "https://account.apple.com/sign-in"
URL_MANAGE   = "https://account.apple.com/account/manage"
URL_DEVICES  = "https://account.apple.com/account/manage/section/devices"
URL_SECURITY = "https://account.apple.com/account/manage/section/security"
URL_FINDMY   = "https://www.icloud.com/find/"
URL_MAIL     = "https://www.icloud.com/mail/"

TIMEOUT = 30_000
LONG    = 60_000

# ── tiny helpers ──────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _fname(step: str) -> str:
    return f"{SCREENSHOTS_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{step}.png"

async def _rnd_delay(lo: float = 0.4, hi: float = 1.8):
    await asyncio.sleep(lo + (hi - lo) * random.random())

async def _shot(page, step: str) -> Optional[bytes]:
    """Take a screenshot, save to disk, return bytes."""
    try:
        data = await page.screenshot(full_page=False)
        fname = _fname(step)
        with open(fname, "wb") as f:
            f.write(data)
        logger.info(f"[{_ts()}] 📸 {fname}")
        return data
    except Exception as e:
        logger.warning(f"[{_ts()}] screenshot failed: {e}")
        return None

async def _get_auth_frame(page, timeout: float = 15.0):
    """Return the idmsa.apple.com iframe (Apple renders login form there)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for f in page.frames:
            if "idmsa.apple.com" in f.url:
                return f
        await asyncio.sleep(0.3)
    return None

async def _type_human(locator, text: str):
    await locator.click()
    await locator.fill("")
    await _rnd_delay(0.1, 0.3)
    for ch in text:
        await locator.type(ch, delay=35 + int(70 * random.random()))
    await _rnd_delay(0.2, 0.4)

async def _wait_visible(frame, selector: str, timeout: float = 10.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            if await frame.locator(selector).first.is_visible():
                return True
        except Exception:
            pass
        await asyncio.sleep(0.3)
    return False

async def _click_first_visible(frame, selectors: List[str], timeout: float = 8.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for sel in selectors:
            try:
                el = frame.locator(sel).first
                if await el.is_visible():
                    await el.click()
                    await _rnd_delay(0.4, 0.9)
                    logger.info(f"[{_ts()}] 🖱️  {sel}")
                    return True
            except Exception:
                pass
        await asyncio.sleep(0.3)
    logger.warning(f"[{_ts()}] ⚠️  none visible: {selectors}")
    return False

# ── browser factory ───────────────────────────────────────────────────────────

async def _make_context(pw, acc_id: int, headless: bool = True):
    profile = os.path.abspath(f"{SESSIONS_DIR}/{acc_id}")
    os.makedirs(profile, exist_ok=True)
    ctx = await pw.chromium.launch_persistent_context(
        profile,
        headless=headless,
        args=[
            "--no-sandbox", "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars", "--lang=ru-RU,ru",
        ],
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1400, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        "window.chrome={runtime:{}};"
    )
    return ctx

# ── apple_signin ──────────────────────────────────────────────────────────────

async def apple_signin(
    acc_id: int,
    email: str,
    password: str,
    q1_text: str = "", q1_answer: str = "",
    q2_text: str = "", q2_answer: str = "",
    q3_text: str = "", q3_answer: str = "",
    tfa_queue=None,
    notify_fn=None,
    _ctx=None,
    _page=None,
) -> Dict[str, Any]:
    """
    Sign in to Apple ID.
    Screenshots every step: page_load, email_typed, after_continue,
    password_typed, after_signin, and any error state.
    """
    from playwright.async_api import async_playwright

    result: Dict[str, Any] = {"ok": False, "error": "", "screenshots": [], "screenshot": None}
    own_pw = _ctx is None
    pw = None

    def _add_shot(data):
        if data:
            result["screenshots"].append(data)
            result["screenshot"] = data   # keep last for compat

    try:
        if own_pw:
            pw = await async_playwright().start()
            ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        else:
            ctx = _ctx

        page = _page or await ctx.new_page()
        page.set_default_timeout(TIMEOUT)

        # ── step 1: load sign-in page ─────────────────────────────────────
        logger.info(f"[{_ts()}] 🌐 {URL_SIGNIN}")
        await page.goto(URL_SIGNIN, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 3.0)
        _add_shot(await _shot(page, "01_page_load"))

        # ── step 2: find iframe ───────────────────────────────────────────
        frame = await _get_auth_frame(page, timeout=15.0)
        if frame is None:
            _add_shot(await _shot(page, "02_no_iframe"))
            result["error"] = "idmsa iframe not found"
            return result

        if not await _wait_visible(frame, "#account_name_text_field", timeout=10.0):
            _add_shot(await _shot(page, "02_no_email_field"))
            result["error"] = "Email field not visible"
            return result

        # ── step 3: type email ────────────────────────────────────────────
        logger.info(f"[{_ts()}] ✍️  email")
        await _type_human(frame.locator("#account_name_text_field").first, email)
        _add_shot(await _shot(page, "03_email_typed"))

        # ── step 4: click Continue ────────────────────────────────────────
        if not await _click_first_visible(frame, ["#sign-in", "button.signin", "button[type='submit']"]):
            _add_shot(await _shot(page, "04_no_continue_btn"))
            result["error"] = "Continue button not found"
            return result
        await _rnd_delay(1.5, 2.5)
        _add_shot(await _shot(page, "04_after_continue"))

        # ── step 5: type password ─────────────────────────────────────────
        if not await _wait_visible(frame, "#password_text_field", timeout=10.0):
            _add_shot(await _shot(page, "05_no_pwd_field"))
            result["error"] = "Password field not visible after Continue"
            return result

        logger.info(f"[{_ts()}] 🔑 password")
        await _type_human(frame.locator("#password_text_field").first, password)
        _add_shot(await _shot(page, "05_password_typed"))

        # ── step 6: click Sign In ─────────────────────────────────────────
        await _click_first_visible(frame, ["#sign-in", "button.signin", "button[type='submit']"])
        await _rnd_delay(1.5, 2.5)
        _add_shot(await _shot(page, "06_after_signin_click"))

        # ── step 7: post-login loop ───────────────────────────────────────
        for iteration in range(20):
            await _rnd_delay(1.5, 2.5)
            url = page.url

            if "account/manage" in url or ("icloud.com" in url and "sign-in" not in url):
                logger.info(f"[{_ts()}] ✅ signed in")
                _add_shot(await _shot(page, "07_signed_in_success"))
                result["ok"] = True
                return result

            frame = await _get_auth_frame(page, timeout=5.0) or frame
            try:
                content = (await frame.content()).lower()
            except Exception:
                content = (await page.content()).lower()

            # wrong password
            if any(x in content for x in [
                "неверный", "incorrect", "wrong password",
                "invalid password", "apple id или пароль",
                "your apple id or password",
            ]):
                logger.error(f"[{_ts()}] ❌ wrong password")
                _add_shot(await _shot(page, f"07_{iteration:02d}_wrong_password"))
                result["error"] = "Неверный пароль Apple ID. Проверьте пароль в /setup."
                return result

            # security questions
            if ("контрольн" in content or "security question" in content
                    or (q1_text and q1_text[:6].lower() in content)
                    or (q2_text and q2_text[:6].lower() in content)
                    or (q3_text and q3_text[:6].lower() in content)):
                logger.info(f"[{_ts()}] 📋 security questions")
                _add_shot(await _shot(page, f"07_{iteration:02d}_security_questions"))
                await _answer_security_questions(
                    frame, q1_text, q1_answer, q2_text, q2_answer, q3_text, q3_answer
                )
                _add_shot(await _shot(page, f"07_{iteration:02d}_after_questions"))
                continue

            # 2FA
            if ("двухфакторн" in content or "two-factor" in content
                    or "безопасность аккаунта" in content):
                logger.info(f"[{_ts()}] 🔒 2FA screen")
                _add_shot(await _shot(page, f"07_{iteration:02d}_2fa"))
                await _click_first_visible(frame, [
                    "button:has-text('другие возможности')",
                    "button:has-text('Другие возможности')",
                    "a:has-text('другие возможности')",
                    "button:has-text('Other Options')",
                ])
                _add_shot(await _shot(page, f"07_{iteration:02d}_after_2fa_click"))
                continue

            # "don't improve"
            if "не улучшать" in content or ("don" in content and "improve" in content):
                logger.info(f"[{_ts()}] 🖱️  не улучшать")
                _add_shot(await _shot(page, f"07_{iteration:02d}_dont_improve"))
                await _click_first_visible(frame, [
                    "button:has-text('не улучшать')",
                    "button:has-text('Не улучшать')",
                ])
                continue

            # trust browser
            if "доверять" in content or "trust this browser" in content:
                logger.info(f"[{_ts()}] 🖱️  trust browser")
                _add_shot(await _shot(page, f"07_{iteration:02d}_trust"))
                await _click_first_visible(frame, [
                    "button:has-text('Доверять')",
                    "button:has-text('Trust')",
                    "#sign-in",
                ])
                continue

        _add_shot(await _shot(page, "07_loop_exceeded"))
        result["error"] = "Login loop exceeded 20 iterations"

    except Exception as e:
        logger.error(f"[{_ts()}] ❌ signin: {e}")
        result["error"] = str(e)
        try:
            _add_shot(await _shot(page, "exception"))
        except Exception:
            pass
    finally:
        if own_pw and pw is not None:
            try:
                await ctx.close()
            except Exception:
                pass
            try:
                await pw.stop()
            except Exception:
                pass

    return result

# ── security questions ────────────────────────────────────────────────────────

async def _answer_security_questions(
    frame,
    q1_text: str, q1_answer: str,
    q2_text: str, q2_answer: str,
    q3_text: str = "", q3_answer: str = "",
):
    """Fill up to 3 security question inputs using exact + partial matching."""
    try:
        inputs = await frame.locator("input[type='text']:visible").all()
        qa_map = {}
        if q1_text and q1_answer:
            qa_map[q1_text] = q1_answer
        if q2_text and q2_answer:
            qa_map[q2_text] = q2_answer
        if q3_text and q3_answer:
            qa_map[q3_text] = q3_answer

        for i, inp in enumerate(inputs[:3]):
            parent_text = await inp.evaluate(
                "el => el.closest('label,div,li,section') "
                "? el.closest('label,div,li,section').innerText : ''"
            )
            matched = None
            for q_text, q_answer in qa_map.items():
                if q_text in parent_text:
                    matched = q_answer; break
                if len(q_text) >= 15 and q_text[:15] in parent_text:
                    matched = q_answer; break
                if q_text[:10] in parent_text:
                    matched = q_answer; break
            if matched:
                await inp.fill(matched)
                logger.info(f"[{_ts()}] ✍️  question {i+1} answered")

        await _click_first_visible(frame, [
            "#sign-in", "button[type='submit']",
            "button:has-text('Продолжить')", "button:has-text('Continue')",
        ])
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ security questions: {e}")


# ── get_devices ───────────────────────────────────────────────────────────────

async def get_devices(acc_id: int, email: str, password: str, **kwargs) -> Dict[str, Any]:
    """Fetch device list from account.apple.com/account/manage/section/devices."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 📱 get_devices")
    result: Dict[str, Any] = {"ok": False, "devices": [], "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_DEVICES, wait_until="networkidle", timeout=LONG)
        await _rnd_delay(2.0, 4.0)
        result["screenshots"].append(await _shot(page, "devices_list"))

        device_items = await page.locator(
            "[class*='device-item'], [class*='DeviceItem'], "
            "[class*='device-list'] li, [role='listitem']"
        ).all()

        devices = []
        for idx, item in enumerate(device_items):
            try:
                await item.click()
                await page.wait_for_load_state("networkidle", timeout=10000)
                await _rnd_delay(1.0, 2.0)
                result["screenshots"].append(await _shot(page, f"device_{idx:02d}_detail"))

                text = await page.inner_text("main, body")
                dev: Dict[str, str] = {"name": "", "model": "", "version": "", "imei": ""}
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                for i, line in enumerate(lines):
                    nxt = lines[i + 1] if i + 1 < len(lines) else ""
                    if line in ("Описание", "Description"):
                        dev["name"] = nxt
                    elif line in ("Модель", "Model"):
                        dev["model"] = nxt
                    elif line in ("Версия", "Version"):
                        dev["version"] = nxt
                    elif line == "IMEI":
                        dev["imei"] = nxt
                if not dev["imei"]:
                    m = re.search(r"\b(\d{15})\b", text)
                    if m:
                        dev["imei"] = m.group(1)
                if dev["name"] or dev["model"]:
                    devices.append(dev)
                    logger.info(f"[{_ts()}] 📱 {dev['name']} | {dev['model']} | {dev['imei']}")
                await page.keyboard.press("Escape")
                await _rnd_delay(0.5, 1.0)
            except Exception as e:
                logger.warning(f"[{_ts()}] ⚠️  device {idx}: {e}")

        result["ok"] = True
        result["devices"] = devices
        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[{_ts()}] ❌ get_devices: {e}")
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result


# ── get_findmy_devices ────────────────────────────────────────────────────────

async def get_findmy_devices(acc_id: int, email: str, password: str, **kwargs) -> Dict[str, Any]:
    """Fetch devices from iCloud Find My."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 📍 get_findmy_devices")
    result: Dict[str, Any] = {"ok": False, "devices": [], "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_FINDMY, wait_until="domcontentloaded", timeout=LONG)
        await _rnd_delay(4.0, 7.0)
        try:
            await page.wait_for_load_state("networkidle", timeout=45000)
        except Exception:
            pass
        result["screenshots"].append(await _shot(page, "findmy_loaded"))

        content = await page.content()
        if "двухфакторн" in content.lower() or "контрольн" in content.lower():
            result["error"] = "Re-authentication required in Find My"
            result["screenshots"].append(await _shot(page, "findmy_reauth"))
            return result

        devices = []
        for sel in ["[class*='device']", "[class*='DeviceList'] li",
                    "div[role='listitem']", "[data-device-id]"]:
            items = await page.locator(sel).all()
            if items:
                for item in items[:20]:
                    try:
                        text = await item.inner_text()
                        if not text or len(text) < 3:
                            continue
                        name = text.split("\n")[0].strip()
                        if not name or name.lower() in ("устройства", "devices"):
                            continue
                        lines = [l.strip() for l in text.split("\n") if l.strip()]
                        devices.append({
                            "name": name,
                            "model": lines[1] if len(lines) > 1 else "",
                            "status": "", "location": "",
                        })
                        logger.info(f"[{_ts()}] 📍 {name}")
                    except Exception:
                        pass
                if devices:
                    break

        result["screenshots"].append(await _shot(page, "findmy_result"))
        result["ok"] = True
        result["devices"] = devices
        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
        logger.info(f"[{_ts()}] ✅ findmy: {len(devices)} devices")
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ get_findmy_devices: {e}")
        result["error"] = str(e)
        try:
            result["screenshots"].append(await _shot(page, "findmy_error"))
        except Exception:
            pass
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result

# ── check_mail ────────────────────────────────────────────────────────────────

async def check_mail(acc_id: int, email: str, password: str, **kwargs) -> Dict[str, Any]:
    """Check iCloud mail inbox. Returns mails list with keys: from, subject, date, is_apple."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 📬 check_mail")
    result: Dict[str, Any] = {"ok": False, "mails": [], "unread": 0, "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_MAIL, wait_until="domcontentloaded", timeout=LONG)
        await _rnd_delay(5.0, 8.0)
        try:
            await page.wait_for_load_state("networkidle", timeout=60000)
        except asyncio.TimeoutError:
            logger.warning(f"[{_ts()}] ⚠️  mail networkidle timeout — continuing")
        result["screenshots"].append(await _shot(page, "mail_loaded"))

        mail_items = await page.locator(
            "[class*='message'], [class*='Message'], "
            "li[role='listitem'], .mail-item"
        ).all()

        mails = []
        unread = 0
        for item in mail_items[:10]:
            try:
                text = await item.inner_text()
                if text and len(text) > 5:
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    is_unread = "unread" in (await item.get_attribute("class") or "").lower()
                    if is_unread:
                        unread += 1
                    mails.append({
                        "from":     lines[0] if lines else "",
                        "sender":   lines[0] if lines else "",   # alias for bot compat
                        "subject":  lines[1] if len(lines) > 1 else "",
                        "date":     lines[2] if len(lines) > 2 else "",
                        "is_apple": "apple" in text.lower() or "icloud" in text.lower(),
                        "unread":   is_unread,
                    })
            except Exception:
                pass

        result["screenshots"].append(await _shot(page, "mail_result"))
        result["ok"] = True
        result["mails"] = mails
        result["unread"] = unread
        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
        logger.info(f"[{_ts()}] ✅ mail: {len(mails)} messages, {unread} unread")
    except asyncio.TimeoutError:
        result["error"] = "Mail load timeout"
        try:
            result["screenshots"].append(await _shot(page, "mail_timeout"))
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ check_mail: {e}")
        result["error"] = str(e)
        try:
            result["screenshots"].append(await _shot(page, "mail_error"))
        except Exception:
            pass
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result


# ── get_security_info ─────────────────────────────────────────────────────────

async def get_security_info(acc_id: int, email: str, password: str, **kwargs) -> Dict[str, Any]:
    """Fetch security settings from account.apple.com/account/manage/section/security."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 🔒 get_security_info")
    result: Dict[str, Any] = {"ok": False, "info": {}, "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_SECURITY, wait_until="networkidle", timeout=LONG)
        await _rnd_delay(2.0, 4.0)
        result["screenshots"].append(await _shot(page, "security_loaded"))

        text = await page.inner_text("main, body")
        info: Dict[str, str] = {
            "two_factor": "включена" if ("двухфакторная" in text.lower() or "two-factor" in text.lower()) else "неизвестно",
            "trusted_phone": "",
            "recovery_key": "есть" if "recovery key" in text.lower() else "нет",
        }
        # try to extract trusted phone
        m = re.search(r"(\+?\d[\d\s\-\(\)]{6,})", text)
        if m:
            info["trusted_phone"] = m.group(1).strip()

        result["screenshots"].append(await _shot(page, "security_result"))
        result["ok"] = True
        result["info"] = info
        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
        logger.info(f"[{_ts()}] ✅ security info: {info}")
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ get_security_info: {e}")
        result["error"] = str(e)
        try:
            result["screenshots"].append(await _shot(page, "security_error"))
        except Exception:
            pass
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result


# ── change_password ───────────────────────────────────────────────────────────

async def change_password(
    acc_id: int, email: str, old_password: str, new_password: str, **kwargs
) -> Dict[str, Any]:
    """Change Apple ID password via account.apple.com."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 🔑 change_password")
    result: Dict[str, Any] = {"ok": False, "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, old_password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_SECURITY, wait_until="networkidle", timeout=LONG)
        await _rnd_delay(2.0, 3.0)
        result["screenshots"].append(await _shot(page, "chpass_security_page"))

        # click "Change Password" button
        clicked = await _click_first_visible(page, [
            "button:has-text('Изменить пароль')",
            "button:has-text('Change Password')",
            "a:has-text('Изменить пароль')",
        ], timeout=10.0)
        if not clicked:
            result["error"] = "Change Password button not found"
            result["screenshots"].append(await _shot(page, "chpass_no_btn"))
            return result

        await _rnd_delay(1.5, 2.5)
        result["screenshots"].append(await _shot(page, "chpass_form"))

        # fill current password
        cur_sel = "input[name*='current'], input[placeholder*='current' i], input[placeholder*='текущий' i]"
        if await _wait_visible(page, cur_sel, timeout=8.0):
            await _type_human(page.locator(cur_sel).first, old_password)

        # fill new password (twice)
        new_sel = "input[name*='new'], input[placeholder*='new' i], input[placeholder*='новый' i]"
        new_inputs = await page.locator(new_sel).all()
        for inp in new_inputs[:2]:
            try:
                if await inp.is_visible():
                    await _type_human(inp, new_password)
            except Exception:
                pass

        result["screenshots"].append(await _shot(page, "chpass_filled"))

        # submit
        await _click_first_visible(page, [
            "button[type='submit']",
            "button:has-text('Изменить')",
            "button:has-text('Change')",
        ], timeout=8.0)
        await _rnd_delay(2.0, 4.0)
        result["screenshots"].append(await _shot(page, "chpass_submitted"))

        content = (await page.content()).lower()
        if any(x in content for x in ["успешно", "success", "изменён", "changed"]):
            result["ok"] = True
            logger.info(f"[{_ts()}] ✅ password changed")
        else:
            result["error"] = "Password change result unclear — check screenshot"

        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ change_password: {e}")
        result["error"] = str(e)
        try:
            result["screenshots"].append(await _shot(page, "chpass_error"))
        except Exception:
            pass
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result


# ── erase_findmy_device ───────────────────────────────────────────────────────

async def erase_findmy_device(
    acc_id: int, email: str, password: str, device_name: str, **kwargs
) -> Dict[str, Any]:
    """Erase a device via iCloud Find My."""
    from playwright.async_api import async_playwright
    logger.info(f"[{_ts()}] 🗑️  erase_findmy_device: {device_name}")
    result: Dict[str, Any] = {"ok": False, "error": "", "screenshots": [], "screenshot": None}

    pw = await async_playwright().start()
    ctx = None
    try:
        ctx = await _make_context(pw, acc_id, headless=HEADLESS)
        page = await ctx.new_page()

        signin = await apple_signin(acc_id, email, password, _ctx=ctx, _page=page, **kwargs)
        result["screenshots"].extend(signin.get("screenshots", []))
        if not signin["ok"]:
            result["error"] = signin["error"]
            result["screenshot"] = signin.get("screenshot")
            return result

        await page.goto(URL_FINDMY, wait_until="domcontentloaded", timeout=LONG)
        await _rnd_delay(5.0, 8.0)
        try:
            await page.wait_for_load_state("networkidle", timeout=45000)
        except Exception:
            pass
        result["screenshots"].append(await _shot(page, "erase_findmy_loaded"))

        # find device by name
        device_el = page.get_by_text(device_name, exact=False).first
        try:
            vis = await device_el.is_visible(timeout=5000)
        except Exception:
            vis = False

        if not vis:
            result["error"] = f"Device '{device_name}' not found in Find My"
            result["screenshots"].append(await _shot(page, "erase_device_not_found"))
            return result

        await device_el.click()
        await _rnd_delay(1.5, 2.5)
        result["screenshots"].append(await _shot(page, "erase_device_selected"))

        # click Erase
        erased = await _click_first_visible(page, [
            "button:has-text('Стереть')",
            "button:has-text('Erase')",
            "button:has-text('Erase iPhone')",
            "button:has-text('Erase iPad')",
            "button:has-text('Erase Mac')",
        ], timeout=10.0)

        if not erased:
            result["error"] = "Erase button not found"
            result["screenshots"].append(await _shot(page, "erase_no_btn"))
            return result

        await _rnd_delay(1.5, 2.5)
        result["screenshots"].append(await _shot(page, "erase_confirm_dialog"))

        # confirm
        await _click_first_visible(page, [
            "button:has-text('Стереть')",
            "button:has-text('Erase')",
            "button:has-text('Confirm')",
            "button:has-text('Подтвердить')",
        ], timeout=8.0)
        await _rnd_delay(2.0, 4.0)
        result["screenshots"].append(await _shot(page, "erase_done"))

        result["ok"] = True
        result["screenshot"] = result["screenshots"][-1] if result["screenshots"] else None
        logger.info(f"[{_ts()}] ✅ erase command sent for: {device_name}")
    except Exception as e:
        logger.error(f"[{_ts()}] ❌ erase_findmy_device: {e}")
        result["error"] = str(e)
        try:
            result["screenshots"].append(await _shot(page, "erase_error"))
        except Exception:
            pass
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        await pw.stop()
    return result


# ── monitor_check ─────────────────────────────────────────────────────────────

async def monitor_check(acc_id: int, email: str, password: str, **kwargs) -> Dict[str, Any]:
    """
    Quick check: sign in, get device list, return it.
    Used by the background monitor loop to detect new devices.
    """
    logger.info(f"[{_ts()}] 🔍 monitor_check")
    return await get_devices(acc_id, email, password, **kwargs)


logger.info("apple_automation.py loaded")
