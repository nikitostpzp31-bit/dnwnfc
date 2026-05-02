"""Apple iCloud automation via Playwright.

Key fixes vs previous version:
- Security questions: use aria-describedby="question-N" to link question text -> input field
- Apple shows questions 2 at a time (not all at once)
- Dynamic input IDs (form-textbox-TIMESTAMP-0) — never use ID for lookup
- iCloud (Find My, Mail) uses the same idmsa iframe as account.apple.com
- Find My: extract devices from JS window data + DOM fallback
- Mail: wait for message list to load before scraping
"""
import asyncio
import os
import random
import re
from datetime import datetime
from typing import Optional

from config import HEADLESS
from logger import get_logger

logger = get_logger()

SESSIONS_DIR    = "pw_sessions"
SCREENSHOTS_DIR = "screenshots"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

URL_SIGNIN   = "https://account.apple.com/sign-in"
URL_MANAGE   = "https://account.apple.com/account/manage"
URL_DEVICES  = "https://account.apple.com/account/manage/section/devices"
URL_SECURITY = "https://account.apple.com/account/manage/section/security"
URL_FINDMY   = "https://www.icloud.com/find/"
URL_MAIL     = "https://www.icloud.com/mail/"

TIMEOUT = 30_000
LONG    = 90_000


def _ts():
    return datetime.now().strftime("%H:%M:%S")


async def _rnd_delay(lo=0.4, hi=1.8):
    await asyncio.sleep(lo + (hi - lo) * random.random())


async def _get_browser(acc_id, headless=True):
    from playwright.async_api import async_playwright
    profile = os.path.abspath(f"{SESSIONS_DIR}/{acc_id}")
    os.makedirs(profile, exist_ok=True)
    pw = await async_playwright().start()
    ctx = await pw.chromium.launch_persistent_context(
        profile,
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--lang=ru-RU,ru",
            "--window-size=1400,900",
            "--disable-infobars",
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
    )
    await ctx.add_init_script("""(() => {
        try{Object.defineProperty(navigator,'webdriver',{get:()=>undefined});}catch(e){}
        try{Object.defineProperty(navigator,'platform',{get:()=>'MacIntel'});}catch(e){}
        try{Object.defineProperty(navigator,'vendor',{get:()=>'Google Inc.'});}catch(e){}
        try{if(!window.chrome)window.chrome={};if(!window.chrome.runtime)window.chrome.runtime={};}catch(e){}
        try{Object.defineProperty(navigator,'languages',{get:()=>['ru-RU','ru','en-US','en']});}catch(e){}
    })();""")
    logger.info(f"[{_ts()}] Browser started (headless={headless})")
    return pw, ctx


async def _new_page(ctx):
    page = await ctx.new_page()
    page.set_default_timeout(TIMEOUT)
    page.set_default_navigation_timeout(LONG)
    return page


async def _screenshot(page, step=""):
    try:
        data = await page.screenshot(full_page=False)
        if step:
            fname = f"{SCREENSHOTS_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{step}.png"
            with open(fname, "wb") as f:
                f.write(data)
            logger.info(f"[{_ts()}] Screenshot: {fname}")
        return data
    except Exception:
        return None


async def _get_idmsa_frame(page):
    for f in page.frames:
        if "idmsa.apple.com" in f.url:
            return f
    return None


async def _wait_idmsa_inputs(page, min_inputs=1, timeout=45):
    """Wait until idmsa frame has at least min_inputs visible inputs."""
    for attempt in range(timeout):
        await asyncio.sleep(1)
        for f in page.frames:
            if "idmsa.apple.com" in f.url:
                try:
                    cnt = await f.evaluate(
                        'document.querySelectorAll("input:not([type=hidden])").length'
                    )
                    if cnt >= min_inputs:
                        logger.info(f"[{_ts()}] idmsa ready: {cnt} inputs (attempt {attempt+1})")
                        return f
                except Exception:
                    pass
        if attempt % 10 == 9:
            logger.info(f"[{_ts()}] Still waiting for idmsa... {attempt+1}s")
    return None


async def _type_frame(frame, selector, text):
    el = frame.locator(selector).first
    await el.click()
    await el.fill("")
    await _rnd_delay(0.1, 0.3)
    for ch in text:
        await el.type(ch, delay=40 + int(80 * random.random()))
    await _rnd_delay(0.2, 0.5)


async def _click_frame_btn(frame, *texts):
    for text in texts:
        for sel in [f"button:has-text('{text}')", f"input[value='{text}']"]:
            try:
                el = frame.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await el.click()
                    return True
            except Exception:
                pass
        try:
            el = frame.get_by_role("button", name=re.compile(text, re.I)).first
            if await el.is_visible(timeout=2000):
                await el.click()
                return True
        except Exception:
            pass
    return False


async def _frame_text(page):
    parts = []
    for f in page.frames:
        try:
            t = await f.evaluate('document.body ? document.body.innerText : ""')
            parts.append(t.lower())
        except Exception:
            pass
    return " ".join(parts)

def _normalize_q(text: str) -> str:
    """Normalize question text for matching: strip, lowercase, unify question marks."""
    return text.strip().lower().replace("？", "?").replace("\u3002", ".").replace("\u3001", ",")


async def _answer_security_questions(frame, q1_text, q1_answer, q2_text, q2_answer,
                                      q3_text="", q3_answer=""):
    """
    Fill security question inputs.

    Apple renders questions inside divs with id="question-N".
    Each input has aria-describedby="question-N" linking it to its question text.
    Apple shows 2 questions per page, then a Continue button, then possibly more.

    Strategy:
    1. Find all [id^="question-"] divs and their linked inputs via aria-describedby
    2. Match question text to known answers (normalized exact -> partial -> positional)
    3. Click Continue, repeat until no more question pages
    """
    # Build normalized lookup: normalized_question -> answer
    qa_map = {}       # normalized -> answer
    qa_raw = {}       # normalized -> original text (for logging)
    for q, a in [(q1_text, q1_answer), (q2_text, q2_answer), (q3_text, q3_answer)]:
        if q and a:
            nq = _normalize_q(q)
            qa_map[nq] = a.strip()
            qa_raw[nq] = q.strip()

    if not qa_map:
        logger.warning(f"[{_ts()}] No security question data provided")
        return False

    logger.info(f"[{_ts()}] Answering security questions ({len(qa_map)} known)")
    qa_values = list(qa_map.values())

    for page_attempt in range(8):
        await asyncio.sleep(2)

        # Extract question-input pairs via aria-describedby
        try:
            pairs = await frame.evaluate("""() => {
                const result = [];
                document.querySelectorAll('[id^="question-"]').forEach(function(qDiv) {
                    const inp = document.querySelector('input[aria-describedby="' + qDiv.id + '"]');
                    if (inp) {
                        result.push({
                            qId: qDiv.id,
                            qText: qDiv.innerText.trim(),
                            inputId: inp.id,
                            visible: inp.offsetParent !== null
                        });
                    }
                });
                return result;
            }""")
        except Exception as e:
            logger.warning(f"[{_ts()}] JS pairs error: {e}")
            pairs = []

        logger.info(f"[{_ts()}] Found {len(pairs)} question-input pairs on page {page_attempt+1}")

        if not pairs:
            # Fallback: find all visible password inputs that aren't the login password field
            try:
                inputs_info = await frame.evaluate("""() =>
                    Array.from(document.querySelectorAll('input[type="password"]'))
                    .filter(el => el.offsetParent !== null && el.id !== 'password_text_field')
                    .map((el, i) => ({id: el.id, idx: i}))
                """)
                logger.info(f"[{_ts()}] Fallback: {len(inputs_info)} password inputs")
                for i, info in enumerate(inputs_info):
                    if i < len(qa_values):
                        try:
                            if info["id"]:
                                el = frame.locator(f'#{info["id"]}').first
                            else:
                                el = frame.locator('input[type="password"]').nth(i)
                            await el.click()
                            await el.fill(qa_values[i])
                            logger.info(f"[{_ts()}] Fallback fill [{i}]: {qa_values[i]}")
                        except Exception as fe:
                            logger.warning(f"[{_ts()}] Fallback fill error: {fe}")
            except Exception as e:
                logger.warning(f"[{_ts()}] Fallback error: {e}")
        else:
            filled = 0
            for i, pair in enumerate(pairs):
                q_on_page     = pair.get("qText", "").strip()
                q_on_page_n   = _normalize_q(q_on_page)
                inp_id        = pair.get("inputId", "")
                q_id          = pair.get("qId", "")

                # Match: normalized exact -> partial (first 8 chars) -> positional
                answer = qa_map.get(q_on_page_n)
                if not answer:
                    for kq, ka in qa_map.items():
                        if len(kq) >= 6 and (kq[:8] in q_on_page_n or q_on_page_n[:8] in kq):
                            answer = ka
                            break
                if not answer and i < len(qa_values):
                    answer = qa_values[i]
                    logger.warning(f"[{_ts()}] Positional answer [{i}]: '{q_on_page[:30]}' -> '{answer}'")

                if answer:
                    try:
                        if inp_id:
                            el = frame.locator(f'#{inp_id}').first
                        else:
                            el = frame.locator(f'input[aria-describedby="{q_id}"]').first
                        await el.click()
                        await el.fill(answer)
                        await _rnd_delay(0.2, 0.5)
                        logger.info(f"[{_ts()}] Filled '{q_on_page[:40]}' -> '{answer}'")
                        filled += 1
                    except Exception as e:
                        logger.warning(f"[{_ts()}] Fill error for '{q_on_page[:30]}': {e}")

            logger.info(f"[{_ts()}] Filled {filled}/{len(pairs)} fields")

        await _rnd_delay(0.5, 1.0)

        # Click Continue / Next
        clicked = await _click_frame_btn(frame, "Continue", "Продолжить", "Далее", "Next", "继续")
        if not clicked:
            try:
                await frame.evaluate("""() => {
                    const btns = Array.from(document.querySelectorAll('button'));
                    const btn = btns.find(b => /continue|продолжить|далее|next|继续/i.test(b.innerText))
                              || btns.find(b => b.type === 'submit');
                    if (btn) btn.click();
                }""")
            except Exception:
                pass

        await _rnd_delay(3.0, 5.0)

        # Check if we're past the questions page
        try:
            body = await frame.evaluate('document.body ? document.body.innerText.toLowerCase() : ""')
            has_questions = (
                "answer your security" in body
                or "security question" in body
                or "контрольн" in body
            )
            wrong_answer = (
                "don't match" in body
                or "incorrect" in body
                or "не совпадают" in body
                or "неверн" in body
            )
            if wrong_answer:
                logger.warning(f"[{_ts()}] Wrong answers detected, retrying page {page_attempt+1}")
                continue
            if not has_questions:
                logger.info(f"[{_ts()}] Security questions passed after {page_attempt+1} pages")
                return True
        except Exception:
            return True

    logger.info(f"[{_ts()}] Security questions: max attempts reached, continuing")
    return True

async def apple_signin(page, email, password, q1_text, q1_answer, q2_text, q2_answer,
                       tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Sign in to account.apple.com.

    Apple's sign-in page loads both email and password fields in the same idmsa
    iframe, but the password field is behind a CSS overlay until after the email
    step is submitted. We must:
      1. Fill email -> click Continue -> wait for overlay to slide away
      2. Then fill password (now clickable) -> click Sign In
    """
    result = {"ok": False, "error": "", "screenshot": None}
    try:
        logger.info(f"[{_ts()}] Navigating to {URL_SIGNIN}")
        await page.goto(URL_SIGNIN, wait_until="domcontentloaded")
        await _rnd_delay(4.0, 6.0)

        # Wait for idmsa iframe
        auth_frame = await _wait_idmsa_inputs(page, min_inputs=1, timeout=45)
        if not auth_frame:
            result["error"] = "Apple sign-in form did not load (no idmsa iframe)"
            result["screenshot"] = await _screenshot(page, "iframe_empty")
            return result

        # --- Step 1: Fill email ---
        email_sel = "#account_name_text_field"
        try:
            el = auth_frame.locator(email_sel).first
            if await el.is_visible(timeout=5000):
                logger.info(f"[{_ts()}] Filling email")
                await el.click()
                await el.fill("")
                await _rnd_delay(0.1, 0.3)
                for ch in email:
                    await el.type(ch, delay=40 + int(80 * random.random()))
                await _rnd_delay(0.3, 0.6)
        except Exception as e:
            logger.warning(f"[{_ts()}] Email fill error: {e}")

        # Click Continue (submits email step)
        clicked = await _click_frame_btn(auth_frame, "Continue", "Продолжить", "Sign In", "Войти")
        if not clicked:
            for sel in ["#sign-in", "button[type='submit']", "button.button-primary"]:
                try:
                    el = auth_frame.locator(sel).first
                    if await el.is_visible(timeout=1000):
                        await el.click()
                        break
                except Exception:
                    pass

        # Wait for CSS transition: overlay slides away, password becomes clickable
        # The password field exists in DOM but is behind .signin-content overlay
        # After Continue click, Apple animates the transition (~1-2s)
        await _rnd_delay(2.5, 4.0)

        # --- Step 2: Fill password ---
        # After Continue, Apple auto-focuses the password field (tabindex becomes 0).
        # The field is already active — type directly via keyboard, no click needed.
        for f in page.frames:
            if "idmsa.apple.com" in f.url:
                auth_frame = f
                break

        # Wait for password field to become active (tabindex 0 = focusable)
        pwd_ready = False
        for attempt in range(12):
            try:
                info = await auth_frame.evaluate("""() => {
                    const el = document.querySelector('#password_text_field');
                    return el ? {tabindex: el.tabIndex, active: document.activeElement === el} : null;
                }""")
                if info and info.get("tabindex", -1) == 0:
                    pwd_ready = True
                    logger.info(f"[{_ts()}] Password field ready (attempt {attempt+1}), active={info.get('active')}")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)

        if not pwd_ready:
            result["error"] = "Password field never became active"
            result["screenshot"] = await _screenshot(page, "password_not_ready")
            return result

        # Type password via keyboard (field is auto-focused by Apple's JS)
        await auth_frame.evaluate("document.querySelector('#password_text_field').focus()")
        await asyncio.sleep(0.3)
        for ch in password:
            await page.keyboard.type(ch, delay=40 + int(80 * random.random()))
        logger.info(f"[{_ts()}] Password typed via keyboard")
        await _rnd_delay(0.5, 1.0)

        # Submit: press Enter (most reliable) or click Sign In button
        await page.keyboard.press("Enter")
        clicked = True
        logger.info(f"[{_ts()}] Submitted password via Enter")

        await _rnd_delay(3.0, 5.0)

        # --- Main auth loop: handle 2FA, security questions, etc. ---
        for iteration in range(30):
            url  = page.url
            body = await _frame_text(page)
            logger.info(f"[{_ts()}] Auth iter {iteration+1}: {url[:70]}")

            # Success
            if ("account/manage" in url
                    or ("account.apple.com" in url
                        and "sign-in" not in url
                        and "security-questions" not in url)):
                logger.info(f"[{_ts()}] Login successful!")
                result["ok"] = True
                return result

            auth_frame = await _get_idmsa_frame(page)

            # Security questions
            sq_triggered = (
                "answer your security" in body
                or "security question" in body
                or "контрольн" in body
                or (q1_text and q1_text[:6].lower() in body)
                or (q2_text and q2_text[:6].lower() in body)
                or (q3_text and q3_text[:6].lower() in body)
            )
            if sq_triggered:
                logger.info(f"[{_ts()}] Security questions detected")
                target = auth_frame if auth_frame else page
                ok = await _answer_security_questions(
                    target, q1_text, q1_answer, q2_text, q2_answer, q3_text, q3_answer
                )
                if not ok:
                    result["error"] = "Failed to answer security questions"
                    result["screenshot"] = await _screenshot(page, "questions_fail")
                    return result
                # Wait for redirect after questions — page body goes empty then redirects
                for _ in range(15):
                    await asyncio.sleep(1)
                    cur_url = page.url
                    if "account/manage" in cur_url or ("account.apple.com" in cur_url and "sign-in" not in cur_url):
                        logger.info(f"[{_ts()}] Redirected after questions: {cur_url}")
                        result["ok"] = True
                        return result
                continue

            # Wrong security question answers
            if "не совпадают" in body or "don't match" in body or "ответы не совпадают" in body:
                result["error"] = "Wrong security question answers — update them via /setup"
                result["screenshot"] = await _screenshot(page, "wrong_answers")
                return result

            # Wrong credentials
            if "incorrect" in body or ("check the account information" in body and "security" not in body):
                result["error"] = "Wrong email or password"
                result["screenshot"] = await _screenshot(page, "wrong_password")
                return result

            # 2FA code required
            if ("verification code" in body
                    or "код подтверждения" in body
                    or "one-time code" in body
                    or "enter the code" in body):
                logger.info(f"[{_ts()}] 2FA code required")
                if notify_fn:
                    await notify_fn("📲 <b>Требуется код 2FA</b>\nВведите: <code>/tfa 123456</code>")
                if tfa_queue:
                    try:
                        code = await asyncio.wait_for(tfa_queue.get(), timeout=120)
                        target = auth_frame if auth_frame else page
                        code_inputs = target.locator(
                            "input[type='number'],input[inputmode='numeric'],input[maxlength='1']"
                        )
                        cnt = await code_inputs.count()
                        if cnt >= 6:
                            for idx, ch in enumerate(code[:6]):
                                await code_inputs.nth(idx).fill(ch)
                                await _rnd_delay(0.05, 0.15)
                        else:
                            single = target.locator(
                                "input[autocomplete='one-time-code'],input[inputmode='numeric']"
                            ).first
                            if await single.is_visible(timeout=2000):
                                await single.fill(code)
                        if auth_frame:
                            await _click_frame_btn(auth_frame, "Continue", "Продолжить", "Verify")
                        await _rnd_delay(2.0, 3.0)
                        continue
                    except asyncio.TimeoutError:
                        result["error"] = "2FA timeout (120s)"
                        result["screenshot"] = await _screenshot(page, "tfa_timeout")
                        return result
                else:
                    result["error"] = "2FA required but no queue"
                    result["screenshot"] = await _screenshot(page, "tfa_no_queue")
                    return result

            # Analytics / "Don't improve" prompt
            if "don't improve" in body or "не улучшать" in body or "improve siri" in body:
                if auth_frame:
                    await _click_frame_btn(auth_frame, "Don't Improve", "Не улучшать", "Not Now")
                await _rnd_delay(1.5, 2.5)
                continue

            # Trust this browser
            if "trust" in body or "доверять" in body:
                if auth_frame:
                    await _click_frame_btn(auth_frame, "Not Now", "Не сейчас", "Trust")
                await _rnd_delay(1.0, 2.0)
                continue

            await _rnd_delay(1.5, 2.5)

        result["error"] = "Login loop exceeded (30 iterations)"
        result["screenshot"] = await _screenshot(page, "signin_loop_exceeded")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] apple_signin error: {e}")
        result["error"] = str(e)
        result["screenshot"] = await _screenshot(page, "signin_exception")
        return result
async def _signin_icloud(page, email, password, q1_text, q1_answer, q2_text, q2_answer,
                         tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Sign in to iCloud (Find My, Mail) — same idmsa iframe as account.apple.com."""
    try:
        await _rnd_delay(2.0, 4.0)
        for iteration in range(30):
            url  = page.url
            body = await _frame_text(page)
            logger.info(f"[{_ts()}] iCloud iter {iteration+1}: {url[:60]}")

            # Already signed in
            if (("icloud.com/find" in url or "icloud.com/mail" in url)
                    and "sign" not in url.lower()):
                logger.info(f"[{_ts()}] iCloud signed in")
                return True

            # Also check if we're on icloud.com main page (redirected after login)
            if "icloud.com" in url and "idmsa" not in url and "sign-in" not in url:
                logger.info(f"[{_ts()}] iCloud main page reached")
                return True

            auth_frame = await _get_idmsa_frame(page)

            if auth_frame:
                # Fill email if empty
                for sel in ["#account_name_text_field", "input[type='text']", "input[type='email']"]:
                    try:
                        el = auth_frame.locator(sel).first
                        if await el.is_visible(timeout=2000):
                            val = await el.input_value()
                            if not val:
                                logger.info(f"[{_ts()}] iCloud: filling email")
                                await _type_frame(auth_frame, sel, email)
                                await _rnd_delay(0.5, 1.0)
                                await _click_frame_btn(auth_frame, "Continue", "Продолжить")
                                for s in ["#sign-in", "button[type='submit']"]:
                                    try:
                                        e2 = auth_frame.locator(s).first
                                        if await e2.is_visible(timeout=1000):
                                            await e2.click()
                                            break
                                    except Exception:
                                        pass
                                await _rnd_delay(2.0, 3.0)
                            break
                    except Exception:
                        pass

                # Fill password if visible
                try:
                    el = auth_frame.locator("#password_text_field").first
                    if await el.is_visible(timeout=2000):
                        val = await el.input_value()
                        if not val:
                            logger.info(f"[{_ts()}] iCloud: filling password")
                            await _type_frame(auth_frame, "#password_text_field", password)
                            await _rnd_delay(0.5, 1.0)
                            await _click_frame_btn(auth_frame, "Sign In", "Войти", "Continue")
                            for s in ["#sign-in", "button[type='submit']"]:
                                try:
                                    e2 = auth_frame.locator(s).first
                                    if await e2.is_visible(timeout=1000):
                                        await e2.click()
                                        break
                                except Exception:
                                    pass
                            await _rnd_delay(2.5, 4.0)
                            continue
                except Exception:
                    pass

            # Security questions
            sq_triggered = (
                "answer your security" in body
                or "security question" in body
                or "контрольн" in body
                or (q1_text and q1_text[:6].lower() in body)
                or (q2_text and q2_text[:6].lower() in body)
                or (q3_text and q3_text[:6].lower() in body)
            )
            if sq_triggered:
                target = auth_frame if auth_frame else page
                await _answer_security_questions(
                    target, q1_text, q1_answer, q2_text, q2_answer, q3_text, q3_answer
                )
                await _rnd_delay(2.0, 3.0)
                continue

            # 2FA
            if ("verification code" in body
                    or "код подтверждения" in body
                    or "one-time code" in body):
                if notify_fn:
                    await notify_fn("📲 Требуется код 2FA. Введите: <code>/tfa 123456</code>")
                if tfa_queue:
                    try:
                        code = await asyncio.wait_for(tfa_queue.get(), timeout=120)
                        target = auth_frame if auth_frame else page
                        code_inputs = target.locator(
                            "input[type='number'],input[inputmode='numeric'],input[maxlength='1']"
                        )
                        cnt = await code_inputs.count()
                        if cnt >= 6:
                            for idx, ch in enumerate(code[:6]):
                                await code_inputs.nth(idx).fill(ch)
                                await _rnd_delay(0.05, 0.15)
                        else:
                            single = target.locator("input[autocomplete='one-time-code']").first
                            if await single.is_visible(timeout=2000):
                                await single.fill(code)
                        if auth_frame:
                            await _click_frame_btn(auth_frame, "Continue", "Продолжить")
                        await _rnd_delay(2.0, 3.0)
                        continue
                    except asyncio.TimeoutError:
                        return False

            if "don't improve" in body or "не улучшать" in body:
                if auth_frame:
                    await _click_frame_btn(auth_frame, "Don't Improve", "Не улучшать", "Not Now")
                await _rnd_delay(1.5, 2.5)
                continue

            if "trust" in body or "доверять" in body:
                if auth_frame:
                    await _click_frame_btn(auth_frame, "Not Now", "Не сейчас")
                await _rnd_delay(1.0, 2.0)
                continue

            if not auth_frame:
                await _wait_idmsa_inputs(page, timeout=8)

            await _rnd_delay(1.5, 2.5)

        return False

    except Exception as e:
        logger.error(f"[{_ts()}] _signin_icloud error: {e}")
        return False

def _parse_device_text(text):
    dev = {"description": "", "model": "", "version": "", "imei": "", "name": ""}
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for i, line in enumerate(lines):
        ll  = line.lower()
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if "описание" in ll or "description" in ll:
            dev["description"] = nxt
            dev["name"] = dev["name"] or nxt
        elif "модель" in ll or "model" in ll:
            dev["model"] = nxt
        elif "версия" in ll or "version" in ll:
            dev["version"] = nxt
        elif "imei" in ll:
            m = re.search(r"(\d[\d\s]{13,17})", line + " " + nxt)
            if m:
                dev["imei"] = m.group(1).replace(" ", "").strip()
        elif re.search(r"(iphone|ipad|mac|ipod|apple watch|airpods)", ll):
            dev["model"] = dev["model"] or line
            dev["name"]  = dev["name"] or line
    if not dev["name"] and lines:
        dev["name"] = lines[0]
    return dev


async def get_devices(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                      tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Get list of devices from account.apple.com/account/manage/section/devices."""
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "devices": [], "error": "", "screenshot": None}
    try:
        r = await apple_signin(
            page, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not r["ok"]:
            result.update(r)
            return result

        logger.info(f"[{_ts()}] Navigating to devices page")
        await page.goto(URL_DEVICES, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 4.0)

        # Wait for device list
        for sel in ["[class*='device']", "[class*='Device']", "ul li"]:
            try:
                await page.wait_for_selector(sel, timeout=10000)
                break
            except Exception:
                pass

        devices = []
        device_items = None
        count = 0
        for locator_str in [
            "[class*='device-item'],[class*='deviceItem']",
            "[class*='device-list'] li",
            "ul li",
            "[role='listitem']",
        ]:
            device_items = page.locator(locator_str)
            count = await device_items.count()
            if count > 0:
                break

        for i in range(min(count, 30)):
            try:
                item = device_items.nth(i)
                await item.click()
                await _rnd_delay(1.0, 2.0)

                panel = None
                for ps in ["[class*='device-detail']", "[class*='panel']", "[class*='modal']",
                           "aside", "[role='dialog']"]:
                    try:
                        p = page.locator(ps).first
                        if await p.is_visible(timeout=3000):
                            panel = p
                            break
                    except Exception:
                        pass

                text = await panel.inner_text() if panel else await page.inner_text("body")
                dev  = _parse_device_text(text)
                if dev.get("model") or dev.get("name"):
                    devices.append(dev)
                    logger.info(f"[{_ts()}] Device: {dev.get('name','?')} IMEI:{dev.get('imei','—')}")

                for cs in ["[class*='close']", "button[aria-label*='close']"]:
                    try:
                        c = page.locator(cs).first
                        if await c.is_visible(timeout=1500):
                            await c.click()
                            await _rnd_delay(0.5, 1.0)
                            break
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"[{_ts()}] Device {i}: {e}")
                continue

        result["ok"]      = True
        result["devices"] = devices
        logger.info(f"[{_ts()}] Devices total: {len(devices)}")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] get_devices error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "devices_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


async def _extract_findmy_devices(page):
    """Extract device list from Find My page — JS window data first, DOM fallback."""
    devices = []

    # Try JS window objects
    try:
        js_devs = await page.evaluate("""() => {
            const sources = [window.__data__, window.FindMyApp, window.iCloudData, window.appData];
            for (const src of sources) {
                if (src && src.devices && Array.isArray(src.devices)) return src.devices;
            }
            const items = document.querySelectorAll(
                '[class*="device-item"],[class*="deviceItem"],li[class*="device"],[data-testid*="device"]'
            );
            return Array.from(items).map(el => ({
                name: (el.querySelector('[class*="name"],[class*="title"]') || el)
                          .innerText?.split('\\n')[0]?.trim() || '',
                status:   el.querySelector('[class*="status"]')?.innerText?.trim()   || '',
                location: el.querySelector('[class*="location"],[class*="subtitle"]')?.innerText?.trim() || '',
                model:    el.querySelector('[class*="model"]')?.innerText?.trim()    || '',
            }));
        }""")
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
        logger.warning(f"[{_ts()}] JS extract: {e}")

    # DOM fallback
    if not devices:
        try:
            for ls in [
                "[class*='device-item'],[class*='deviceItem']",
                "[class*='device-list'] li",
                "li[class*='device']",
                "ul li",
            ]:
                items = page.locator(ls)
                count = await items.count()
                if count > 0:
                    for i in range(min(count, 30)):
                        try:
                            text  = await items.nth(i).inner_text()
                            if text.strip():
                                lines = [l.strip() for l in text.splitlines() if l.strip()]
                                devices.append({
                                    "name":     lines[0] if lines else "Device",
                                    "status":   lines[1] if len(lines) > 1 else "",
                                    "location": lines[2] if len(lines) > 2 else "",
                                    "model":    "",
                                    "imei":     "",
                                })
                        except Exception:
                            pass
                    if devices:
                        break
        except Exception as e:
            logger.warning(f"[{_ts()}] DOM extract: {e}")

    return devices


async def get_findmy_devices(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                             tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Open icloud.com/find and return device list."""
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "devices": [], "error": "", "screenshot": None}
    try:
        await page.goto(URL_FINDMY, wait_until="domcontentloaded")
        ok = await _signin_icloud(
            page, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not ok:
            result["error"]      = "Failed to sign in to Find My"
            result["screenshot"] = await _screenshot(page, "findmy_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)
        for sel in ["[class*='device']", "[class*='Device']", "ul li", "[class*='map']"]:
            try:
                await page.wait_for_selector(sel, timeout=15000)
                break
            except Exception:
                pass

        devices = await _extract_findmy_devices(page)
        result["ok"]      = True
        result["devices"] = devices
        logger.info(f"[{_ts()}] Find My: {len(devices)} devices")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] get_findmy_devices error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "findmy_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


def _fuzzy_match(name, text):
    words  = name.lower().split()
    text_l = text.lower()
    return sum(1 for w in words if w in text_l) >= max(1, len(words) // 2)


async def erase_findmy_device(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                              device_name, tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Find device by name in Find My and erase it."""
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "error": "", "screenshot": None}
    try:
        await page.goto(URL_FINDMY, wait_until="domcontentloaded")
        ok = await _signin_icloud(
            page, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not ok:
            result["error"]      = "Failed to sign in to Find My"
            result["screenshot"] = await _screenshot(page, "erase_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)
        found = False
        for ls in [
            "[class*='device-item'],[class*='deviceItem']",
            "[class*='device-list'] li",
            "li[class*='device']",
            "ul li",
        ]:
            items = page.locator(ls)
            count = await items.count()
            if count == 0:
                continue
            for i in range(min(count, 30)):
                try:
                    item = items.nth(i)
                    text = await item.inner_text()
                    if device_name.lower() in text.lower() or _fuzzy_match(device_name, text):
                        await item.click()
                        found = True
                        await _rnd_delay(1.0, 2.0)
                        break
                except Exception:
                    pass
            if found:
                break

        if not found:
            result["error"]      = f"Device '{device_name}' not found"
            result["screenshot"] = await _screenshot(page, "erase_not_found")
            return result

        erased = False
        for text in ["Стереть", "Erase", "Erase Device"]:
            try:
                btn = page.get_by_role("button", name=re.compile(text, re.I)).first
                if await btn.is_visible(timeout=3000):
                    await btn.click()
                    erased = True
                    break
            except Exception:
                pass

        if not erased:
            result["error"]      = "Erase button not found"
            result["screenshot"] = await _screenshot(page, "erase_btn_not_found")
            return result

        await _rnd_delay(1.5, 2.5)
        for text in ["Стереть", "Erase", "Подтвердить", "Confirm"]:
            try:
                btn = page.get_by_role("button", name=re.compile(text, re.I)).first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    break
            except Exception:
                pass

        await _rnd_delay(2.0, 3.0)
        result["ok"] = True
        logger.info(f"[{_ts()}] Device '{device_name}' erased!")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] erase_findmy_device error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "erase_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()

async def change_password(acc_id, email, new_password, q1_text, q1_answer, q2_text, q2_answer,
                          tfa_queue=None, notify_fn=None, password="", current_password="",
                          q3_text="", q3_answer=""):
    """Change Apple ID password via account.apple.com."""
    if not current_password:
        current_password = password
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "last_updated": "", "error": "", "screenshot": None}
    try:
        r = await apple_signin(
            page, email, current_password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not r["ok"]:
            result.update(r)
            return result

        await page.goto(URL_MANAGE, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 3.0)

        for text in ["Пароль", "Password"]:
            try:
                btn = page.get_by_role("button", name=re.compile(text, re.I)).first
                if await btn.is_visible(timeout=3000):
                    await btn.click()
                    break
            except Exception:
                pass

        await _rnd_delay(1.5, 2.5)
        pwd_inputs = page.locator("input[type='password']")
        count = await pwd_inputs.count()
        logger.info(f"[{_ts()}] Password fields found: {count}")

        if count >= 3:
            await pwd_inputs.nth(0).fill(current_password)
            await _rnd_delay(0.3, 0.6)
            await pwd_inputs.nth(1).fill(new_password)
            await _rnd_delay(0.3, 0.6)
            await pwd_inputs.nth(2).fill(new_password)
        elif count == 2:
            await pwd_inputs.nth(0).fill(new_password)
            await pwd_inputs.nth(1).fill(new_password)
        else:
            result["error"]      = f"Only {count} password fields found"
            result["screenshot"] = await _screenshot(page, "changepass_fields")
            return result

        await _rnd_delay(0.5, 1.0)
        for sel in ["button[type='submit']", "button[class*='primary']"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    break
            except Exception:
                pass

        await _rnd_delay(2.0, 4.0)
        result["last_updated"] = datetime.now().strftime("%d.%m.%Y")
        result["ok"] = True
        logger.info(f"[{_ts()}] Password changed!")
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] change_password error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "changepass_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


async def _extract_mails(page):
    mails = []
    try:
        for ls in [
            "[class*='message-row'],[class*='MessageRow']",
            "[class*='mail-item'],[class*='MailItem']",
            "[class*='message-list'] li",
            "ul[class*='mail'] li",
        ]:
            items = page.locator(ls)
            count = await items.count()
            if count > 0:
                for i in range(min(count, 15)):
                    try:
                        item  = items.nth(i)
                        text  = await item.inner_text()
                        lines = [l.strip() for l in text.splitlines() if l.strip()]
                        cls   = await item.get_attribute("class") or ""
                        is_unread = "unread" in cls.lower()
                        sender  = lines[0] if lines else ""
                        subject = lines[1] if len(lines) > 1 else ""
                        date    = lines[-1] if len(lines) > 2 else ""
                        is_apple = any(
                            kw in (sender + subject).lower()
                            for kw in ["apple", "icloud", "security", "sign in",
                                       "new device", "password", "пароль", "appleid"]
                        )
                        mails.append({
                            "sender":   sender,
                            "subject":  subject,
                            "date":     date,
                            "unread":   is_unread,
                            "is_apple": is_apple,
                        })
                    except Exception:
                        pass
                break
    except Exception as e:
        logger.warning(f"[{_ts()}] _extract_mails: {e}")
    return mails


async def check_mail(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                     tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Open icloud.com/mail and return inbox messages."""
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "mails": [], "unread": 0, "error": "", "screenshot": None}
    try:
        await page.goto(URL_MAIL, wait_until="domcontentloaded")
        ok = await _signin_icloud(
            page, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not ok:
            result["error"]      = "Failed to sign in to iCloud Mail"
            result["screenshot"] = await _screenshot(page, "mail_login_fail")
            return result

        await _rnd_delay(3.0, 5.0)
        for sel in ["[class*='message'],[class*='mail-item']", "[class*='MessageList']", "ul li"]:
            try:
                await page.wait_for_selector(sel, timeout=15000)
                break
            except Exception:
                pass

        mails  = await _extract_mails(page)
        unread = sum(1 for m in mails if m.get("unread"))
        result["ok"]     = True
        result["mails"]  = mails[:10]
        result["unread"] = unread
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] check_mail error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "mail_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


async def get_security_info(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                            tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Get security settings from account.apple.com/account/manage/section/security."""
    pw, ctx = await _get_browser(acc_id, headless=HEADLESS)
    page = await _new_page(ctx)
    result = {"ok": False, "info": "", "error": "", "screenshot": None}
    try:
        r = await apple_signin(
            page, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not r["ok"]:
            result.update(r)
            return result

        await page.goto(URL_SECURITY, wait_until="domcontentloaded")
        await _rnd_delay(2.0, 3.0)
        for sel in ["[class*='security']", "main"]:
            try:
                await page.wait_for_selector(sel, timeout=TIMEOUT)
                break
            except Exception:
                pass

        info = await page.inner_text("main, [class*='content'], body")
        result["ok"]   = True
        result["info"] = info[:3000]
        return result

    except Exception as e:
        logger.error(f"[{_ts()}] get_security_info error: {e}")
        result["error"]      = str(e)
        result["screenshot"] = await _screenshot(page, "security_error")
        return result
    finally:
        await ctx.close()
        await pw.stop()


async def monitor_check(acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
                        known_devices, tfa_queue=None, notify_fn=None, q3_text="", q3_answer=""):
    """Lightweight Find My check for the monitor loop. Returns new devices."""
    result = {"ok": False, "devices": [], "new_devices": [], "error": ""}
    try:
        logger.info(f"[{_ts()}] Monitor: checking Find My...")
        fm = await get_findmy_devices(
            acc_id, email, password, q1_text, q1_answer, q2_text, q2_answer,
            tfa_queue, notify_fn, q3_text, q3_answer
        )
        if not fm["ok"]:
            result["error"] = fm["error"]
            return result

        current = fm["devices"]
        result["devices"] = current
        result["ok"]      = True

        known_names = {d.get("name", "").lower() for d in known_devices}
        for dev in current:
            name = dev.get("name", "").lower()
            if name and name not in known_names:
                logger.info(f"[{_ts()}] NEW DEVICE: {dev.get('name')}")
                result["new_devices"].append(dev)

        return result

    except Exception as e:
        logger.error(f"[{_ts()}] monitor_check error: {e}")
        result["error"] = str(e)
        return result
