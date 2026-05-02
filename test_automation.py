#!/usr/bin/env python3
"""
End-to-end tests for apple_automation.py
Opens a real browser, navigates to real Apple/iCloud pages,
clicks every button, screenshots every step.

Run:  python3 test_automation.py
"""
import asyncio, glob, os, sys, time

os.environ.setdefault("TELEGRAM_TOKEN", "dummy")
os.environ.setdefault("OWNER_TELEGRAM_ID", "0")
os.environ.setdefault("FERNET_KEY", "")
sys.path.insert(0, os.path.dirname(__file__))

import apple_automation as aa

# ── runner ────────────────────────────────────────────────────────────────────
_results = []

def _ok(name, detail=""):
    print(f"  PASS  {name}" + (f"  [{detail}]" if detail else ""))
    _results.append((name, True, str(detail)))

def _fail(name, detail=""):
    print(f"  FAIL  {name}" + (f"  [{detail}]" if detail else ""))
    _results.append((name, False, str(detail)))

def check(name, cond, detail=""):
    (_ok if cond else _fail)(name, str(detail) if detail else "")

def section(title):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")

# ── shared browser (created once) ─────────────────────────────────────────────
_pw = _ctx = _page = None

async def _setup():
    global _pw, _ctx, _page
    from playwright.async_api import async_playwright
    _pw  = await async_playwright().start()
    _ctx = await aa._make_context(_pw, acc_id=1, headless=True)
    _page = await _ctx.new_page()

async def _teardown():
    for obj in (_ctx, _pw):
        try:
            await obj.close() if hasattr(obj, 'close') else await obj.stop()
        except Exception:
            pass

# ── helpers ───────────────────────────────────────────────────────────────────

def _count_shots(prefix=""):
    return len(glob.glob(f"{aa.SCREENSHOTS_DIR}/*{prefix}*.png"))


# =============================================================================
# TEST 1 — module exports & helpers
# =============================================================================
async def test_helpers():
    section("TEST 1 — module exports & helpers")
    for fn in ("apple_signin","get_devices","get_findmy_devices","check_mail",
               "get_security_info","change_password","erase_findmy_device","monitor_check"):
        check(f"function {fn} exported", callable(getattr(aa, fn, None)))

    ts = aa._ts()
    check("_ts() HH:MM:SS", len(ts)==8 and ts.count(":")==2, ts)

    t0 = time.monotonic()
    await aa._rnd_delay(0.05, 0.08)
    check("_rnd_delay waits", 0.04 <= time.monotonic()-t0 <= 0.2)

    check("SESSIONS_DIR exists",    os.path.isdir(aa.SESSIONS_DIR))
    check("SCREENSHOTS_DIR exists", os.path.isdir(aa.SCREENSHOTS_DIR))


# =============================================================================
# TEST 2 — _shot saves PNG to disk
# =============================================================================
async def test_shot():
    section("TEST 2 — _shot saves PNG to disk")
    await _page.set_content("<html><body><h1>shot test</h1></body></html>")
    before = _count_shots("shot_test")
    data = await aa._shot(_page, "shot_test")
    check("returns bytes",        isinstance(data, bytes) and len(data) > 100, f"{len(data)}b")
    check("file written to disk", _count_shots("shot_test") > before)


# =============================================================================
# TEST 3 — _make_context: profile dir + stealth
# =============================================================================
async def test_make_context():
    section("TEST 3 — _make_context: profile dir + stealth")
    from playwright.async_api import async_playwright
    pw2 = await async_playwright().start()
    try:
        ctx2 = await aa._make_context(pw2, acc_id=9001, headless=True)
        check("profile dir created", os.path.isdir(os.path.abspath(f"{aa.SESSIONS_DIR}/9001")))
        p2 = await ctx2.new_page()
        await p2.goto("about:blank")
        wd = await p2.evaluate("navigator.webdriver")
        check("navigator.webdriver undefined (stealth)", wd is None, str(wd))
        await ctx2.close()
    finally:
        await pw2.stop()


# =============================================================================
# TEST 4 — _wait_visible: timeout + dynamic appearance
# =============================================================================
async def test_wait_visible():
    section("TEST 4 — _wait_visible timeout + dynamic appearance")
    await _page.set_content("<html><body><div id='box' style='display:none'>hi</div></body></html>")
    frame = _page.main_frame

    t0 = time.monotonic()
    res = await aa._wait_visible(frame, "#box", timeout=1.0)
    elapsed = time.monotonic() - t0
    check("returns False for hidden element", not res)
    check("respects 1s timeout",              elapsed < 2.0, f"{elapsed:.2f}s")

    await _page.evaluate("document.getElementById('box').style.display='block'")
    check("returns True after element shown", await aa._wait_visible(frame, "#box", timeout=3.0))


# =============================================================================
# TEST 5 — _click_first_visible: skips hidden, fallback, returns False
# =============================================================================
async def test_click_first_visible():
    section("TEST 5 — _click_first_visible selector fallback")
    await _page.set_content("""
        <html><body>
          <button id="b1" style="display:none">Hidden</button>
          <button id="b2">Visible</button>
        </body></html>""")
    frame = _page.main_frame

    ok = await aa._click_first_visible(frame, ["#b1", "#b2"], timeout=3.0)
    check("skips hidden, clicks first visible", ok)

    ok2 = await aa._click_first_visible(frame, ["#nonexistent"], timeout=1.0)
    check("returns False when nothing found", not ok2)


# =============================================================================
# TEST 6 — _get_auth_frame: finds idmsa iframe on sign-in page
# =============================================================================
async def test_get_auth_frame():
    section("TEST 6 — _get_auth_frame finds idmsa.apple.com iframe")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    await aa._shot(_page, "t06_signin_page")

    frame = await aa._get_auth_frame(_page, timeout=15.0)
    check("iframe found",              frame is not None, frame.url[:70] if frame else "None")
    check("iframe is idmsa.apple.com", frame is not None and "idmsa.apple.com" in frame.url)


# =============================================================================
# TEST 7 — sign-in page: all fields and button visible
# =============================================================================
async def test_signin_fields_visible():
    section("TEST 7 — sign-in page: email, password, button visible")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    frame = await aa._get_auth_frame(_page, timeout=15.0)
    if not frame:
        _fail("iframe (prerequisite)"); return

    await aa._shot(_page, "t07_signin_initial")

    email_vis = await aa._wait_visible(frame, "#account_name_text_field", timeout=8.0)
    check("email field visible",    email_vis)

    pwd_vis = await aa._wait_visible(frame, "#password_text_field", timeout=5.0)
    check("password field visible", pwd_vis)

    btn = frame.locator("#sign-in").first
    btn_vis = await btn.is_visible()
    btn_txt = (await btn.inner_text()).strip() if btn_vis else ""
    check("sign-in button visible",          btn_vis, btn_txt)
    check("button text is 'Продолжить'",     btn_txt == "Продолжить", btn_txt)


# =============================================================================
# TEST 8 — type email → click Continue → form updates
# =============================================================================
async def test_email_step():
    section("TEST 8 — type email → click Continue → form updates")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    frame = await aa._get_auth_frame(_page, timeout=15.0)
    if not frame:
        _fail("iframe (prerequisite)"); return

    # type email
    await aa._type_human(frame.locator("#account_name_text_field").first, "test_e2e@icloud.com")
    val = await frame.locator("#account_name_text_field").first.input_value()
    check("email typed correctly", val == "test_e2e@icloud.com", val)
    await aa._shot(_page, "t08_email_typed")

    # click Continue
    clicked = await aa._click_first_visible(frame, ["#sign-in"], timeout=5.0)
    check("Continue button clicked", clicked)
    await asyncio.sleep(2)
    await aa._shot(_page, "t08_after_continue")

    # remember-me appears
    remember = await aa._wait_visible(frame, "#remember-me", timeout=8.0)
    check("remember-me checkbox appears", remember)

    # button text → "Войти"
    btn_txt = (await frame.locator("#sign-in").first.inner_text()).strip()
    check("button text changes to 'Войти'", btn_txt == "Войти", btn_txt)

    # forgot password link
    forgot = frame.locator("a[href*='iforgot.apple.com']").first
    check("'Forgot password' link visible", await forgot.is_visible())
    await aa._shot(_page, "t08_after_continue_full")


# =============================================================================
# TEST 9 — remember-me checkbox click (via label)
# =============================================================================
async def test_remember_me_checkbox():
    section("TEST 9 — remember-me checkbox click via label")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    frame = await aa._get_auth_frame(_page, timeout=15.0)
    if not frame:
        _fail("iframe (prerequisite)"); return

    await aa._type_human(frame.locator("#account_name_text_field").first, "cb_test@icloud.com")
    await aa._click_first_visible(frame, ["#sign-in"], timeout=5.0)
    await asyncio.sleep(2)

    remember_vis = await aa._wait_visible(frame, "#remember-me", timeout=8.0)
    check("remember-me visible", remember_vis)
    if not remember_vis:
        return

    before = await frame.locator("#remember-me").first.is_checked()
    # click via label (span intercepts direct click on input)
    await frame.locator("label[for='remember-me']").first.click()
    await asyncio.sleep(0.5)
    after = await frame.locator("#remember-me").first.is_checked()
    check("checkbox state toggled via label", before != after, f"{before}→{after}")
    await aa._shot(_page, "t09_remember_me_toggled")


# =============================================================================
# TEST 10 — full button sequence: email→Continue→password→Войти
# =============================================================================
async def test_full_signin_sequence():
    section("TEST 10 — full button sequence on sign-in page")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    frame = await aa._get_auth_frame(_page, timeout=15.0)
    if not frame:
        _fail("iframe (prerequisite)"); return

    # step 1: email
    await aa._type_human(frame.locator("#account_name_text_field").first, "seq_test@icloud.com")
    check("step1: email typed",
          (await frame.locator("#account_name_text_field").first.input_value()) == "seq_test@icloud.com")
    await aa._shot(_page, "t10_step1_email")

    # step 2: Continue
    check("step2: Continue clicked",
          await aa._click_first_visible(frame, ["#sign-in"], timeout=5.0))
    await asyncio.sleep(2)
    await aa._shot(_page, "t10_step2_after_continue")

    # step 3: password field visible
    pwd_vis = await aa._wait_visible(frame, "#password_text_field", timeout=8.0)
    check("step3: password field visible", pwd_vis)

    # step 4: type password
    if pwd_vis:
        await aa._type_human(frame.locator("#password_text_field").first, "FakePass123!")
        check("step4: password typed",
              (await frame.locator("#password_text_field").first.input_value()) == "FakePass123!")
        await aa._shot(_page, "t10_step4_password")

    # step 5: click Войти
    btn_txt = (await frame.locator("#sign-in").first.inner_text()).strip()
    check("step5: button is 'Войти'", btn_txt == "Войти", btn_txt)
    check("step5: Войти clicked",
          await aa._click_first_visible(frame, ["#sign-in"], timeout=5.0))
    await asyncio.sleep(2)
    await aa._shot(_page, "t10_step5_after_signin")

    url = _page.url
    check("step5: still on apple.com", "apple.com" in url, url)


# =============================================================================
# TEST 11 — apple_signin detects wrong password (live site)
# =============================================================================
async def test_signin_wrong_password():
    section("TEST 11 — apple_signin detects wrong password (live)")
    before = _count_shots("wrong_password")
    res = await aa.apple_signin(
        acc_id=5001,
        email="notreal_xyz_test@icloud.com",
        password="WrongPass999!",
    )
    check("returns dict",           isinstance(res, dict))
    check("ok=False",               res["ok"] is False)
    check("error message set",      len(res.get("error","")) > 0, res.get("error","")[:80])
    check("screenshots list present", isinstance(res.get("screenshots"), list))
    check("at least 3 screenshots taken", len(res.get("screenshots",[])) >= 3,
          str(len(res.get("screenshots",[]))))
    check("wrong_password PNG saved", _count_shots("wrong_password") > before)


# =============================================================================
# TEST 12 — apple_signin with empty email returns error fast
# =============================================================================
async def test_signin_empty_email():
    section("TEST 12 — apple_signin with empty email")
    res = await aa.apple_signin(acc_id=5002, email="", password="anything")
    check("ok=False",          res["ok"] is False)
    check("error message set", len(res.get("error","")) > 0, res.get("error",""))
    check("screenshots taken", len(res.get("screenshots",[])) >= 1)


# =============================================================================
# TEST 13 — _answer_security_questions: exact + partial match
# =============================================================================
async def test_security_questions():
    section("TEST 13 — _answer_security_questions fills answers")
    await _page.set_content("""
        <html><body>
          <div>Любимый город детства?<input type='text' id='q1'/></div>
          <div>Кличка первого питомца?<input type='text' id='q2'/></div>
          <div>Девичья фамилия матери?<input type='text' id='q3'/></div>
          <button id='sign-in'>Продолжить</button>
        </body></html>""")
    frame = _page.main_frame

    await aa._answer_security_questions(
        frame,
        q1_text="Любимый город детства?",  q1_answer="Москва",
        q2_text="Кличка первого питомца?",  q2_answer="Барсик",
        q3_text="Девичья фамилия матери?",  q3_answer="Иванова",
    )
    check("q1 filled", (await _page.locator("#q1").input_value()) == "Москва")
    check("q2 filled", (await _page.locator("#q2").input_value()) == "Барсик")
    check("q3 filled", (await _page.locator("#q3").input_value()) == "Иванова")
    await aa._shot(_page, "t13_questions_filled")

    # partial match (10-char prefix)
    await _page.set_content("""
        <html><body>
          <div>Любимый город детства — введите ответ:<input type='text' id='q1'/></div>
          <button id='sign-in'>Продолжить</button>
        </body></html>""")
    frame = _page.main_frame
    await aa._answer_security_questions(
        frame,
        q1_text="Любимый город детства?", q1_answer="Питер",
        q2_text="", q2_answer="",
    )
    check("q1 partial match", (await _page.locator("#q1").input_value()) == "Питер")


# =============================================================================
# TEST 14 — /account/manage redirects to sign-in
# =============================================================================
async def test_manage_redirect():
    section("TEST 14 — /account/manage redirects to sign-in")
    await _page.goto(aa.URL_MANAGE, wait_until="domcontentloaded")
    await asyncio.sleep(2)
    await aa._shot(_page, "t14_manage_redirect")
    url = _page.url
    check("redirected (sign-in or landing)", "sign-in" in url or "account.apple.com" in url, url)

    btn = _page.locator("button.button-elevated, a[href='/sign-in']").first
    check("'Войти в Аккаунт' button visible", await btn.is_visible())


# =============================================================================
# TEST 15 — /account/manage/section/devices redirects + iframe
# =============================================================================
async def test_devices_redirect():
    section("TEST 15 — /devices redirects to sign-in with iframe")
    await _page.goto(aa.URL_DEVICES, wait_until="domcontentloaded")
    await asyncio.sleep(2)
    await aa._shot(_page, "t15_devices_redirect")
    url = _page.url
    check("devices redirects to sign-in", "sign-in" in url, url)

    frame = await aa._get_auth_frame(_page, timeout=10.0)
    check("sign-in iframe present",  frame is not None)
    if frame:
        check("email field visible", await aa._wait_visible(frame, "#account_name_text_field", 5.0))


# =============================================================================
# TEST 16 — iCloud Find My page loads
# =============================================================================
async def test_findmy_page():
    section("TEST 16 — iCloud Find My page loads")
    await _page.goto(aa.URL_FINDMY, wait_until="domcontentloaded")
    await asyncio.sleep(4)
    await aa._shot(_page, "t16_findmy_loaded")
    url = _page.url
    check("Find My URL loaded",  "icloud.com" in url, url)
    title = await _page.title()
    check("page has title",      len(title) > 0, title)
    content = await _page.content()
    check("iCloud content present", "icloud" in content.lower() or "find" in content.lower())
    privacy = _page.locator("a[href*='privacy']").first
    try:
        vis = await privacy.is_visible(timeout=3000)
    except Exception:
        vis = False
    check("privacy link present (page loaded)", vis)


# =============================================================================
# TEST 17 — iCloud Mail page loads
# =============================================================================
async def test_mail_page():
    section("TEST 17 — iCloud Mail page loads")
    await _page.goto(aa.URL_MAIL, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    await aa._shot(_page, "t17_mail_loaded")
    url = _page.url
    check("Mail URL loaded", "icloud.com" in url, url)
    title = await _page.title()
    check("page has title",  len(title) > 0, title)


# =============================================================================
# TEST 18 — privacy link in sign-in iframe
# =============================================================================
async def test_privacy_link_in_iframe():
    section("TEST 18 — privacy link in sign-in iframe")
    await _page.goto(aa.URL_SIGNIN, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    frame = await aa._get_auth_frame(_page, timeout=15.0)
    if not frame:
        _fail("iframe (prerequisite)"); return

    privacy = frame.locator("a[href*='privacy']").first
    try:
        vis = await privacy.is_visible(timeout=3000)
    except Exception:
        vis = False
    check("privacy link visible in iframe", vis)
    if vis:
        href = await privacy.get_attribute("href") or ""
        check("privacy link has href", bool(href), href[:60])
    await aa._shot(_page, "t18_privacy_link")


# =============================================================================
# TEST 19 — screenshots are saved for each step of apple_signin
# =============================================================================
async def test_signin_screenshots_per_step():
    section("TEST 19 — apple_signin saves screenshot at every step")
    res = await aa.apple_signin(
        acc_id=5003,
        email="steps_test@icloud.com",
        password="AnyPass123!",
    )
    shots = res.get("screenshots", [])
    check("screenshots list returned",    isinstance(shots, list))
    check("at least 5 step screenshots",  len(shots) >= 5, str(len(shots)))
    check("last screenshot in 'screenshot' key", res.get("screenshot") == shots[-1] if shots else True)

    # verify named files on disk
    for step in ("01_page_load", "03_email_typed", "04_after_continue", "05_password_typed"):
        found = len(glob.glob(f"{aa.SCREENSHOTS_DIR}/*{step}*")) > 0
        check(f"file *{step}* saved", found)


# =============================================================================
# TEST 20 — _type_human types each character
# =============================================================================
async def test_type_human():
    section("TEST 20 — _type_human types text character by character")
    await _page.set_content("<html><body><input id='inp' type='text'/></body></html>")
    locator = _page.locator("#inp").first
    await aa._type_human(locator, "Hello123")
    val = await _page.locator("#inp").first.input_value()
    check("text typed correctly", val == "Hello123", val)


# =============================================================================
# MAIN
# =============================================================================
async def run_all():
    await _setup()
    try:
        await test_helpers()
        await test_shot()
        await test_make_context()
        await test_wait_visible()
        await test_click_first_visible()
        await test_get_auth_frame()
        await test_signin_fields_visible()
        await test_email_step()
        await test_remember_me_checkbox()
        await test_full_signin_sequence()
        await test_signin_wrong_password()
        await test_signin_empty_email()
        await test_security_questions()
        await test_manage_redirect()
        await test_devices_redirect()
        await test_findmy_page()
        await test_mail_page()
        await test_privacy_link_in_iframe()
        await test_signin_screenshots_per_step()
        await test_type_human()
    finally:
        await _teardown()

    print(f"\n{'='*60}")
    passed = sum(1 for _,ok,_ in _results if ok)
    failed = sum(1 for _,ok,_ in _results if not ok)
    print(f"  Results: {passed}/{len(_results)} passed", end="")
    if failed:
        print(f"  ({failed} FAILED)\n\n  Failed:")
        for name,ok,detail in _results:
            if not ok:
                print(f"    FAIL  {name}  [{detail}]")
    else:
        print("  — all passed")
    print(f"{'='*60}")
    return failed == 0

if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run_all()) else 1)
