

# import os
# import time
# import json
# import logging
# import random
# from datetime import datetime
# import pyautogui

# from selenium.webdriver.chrome.webdriver import WebDriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException, WebDriverException

# logger = logging.getLogger(__name__)
# pyautogui.FAILSAFE = True

# def _find_challenge_host(driver):
#     """
#     Tries multiple strategies to find the most probable host element:
#     1) iframe with src contains challenges.cloudflare.com
#     2) div that looks like host (id patterns like wNUym6 or common container)
#     3) element containing text 'Verify you are human' / 'Xác minh bạn'
#     Returns the WebElement or None.
#     """
#     # 1) look for challenge iframe by src
#     try:
#         iframes = driver.find_elements(By.TAG_NAME, "iframe")
#         for f in iframes:
#             src = (f.get_attribute("src") or "").lower()
#             if any(p in src for p in ("challenges.cloudflare.com", "challenge-platform", "cf-chl")):
#                 # prefer iframe with visible rect later (caller should check rect)
#                 return f
#     except Exception:
#         pass

#     # 2) try host container ids/classes (from screenshots)
#     try:
#         candidates = [
#             "//*[@id='wNUym6']",
#             "//*[contains(@class,'cb-c') or contains(@class,'cb-wrapper') or contains(@class,'challenge')]",
#         ]
#         for xp in candidates:
#             elems = driver.find_elements(By.XPATH, xp)
#             if elems:
#                 return elems[0]
#     except Exception:
#         pass

#     # 3) try textual cues
#     try:
#         texts = [
#             "verify you are human",
#             "xác minh bạn là con người",
#             "verify you are a human",
#         ]
#         page_text = driver.page_source.lower()
#         for t in texts:
#             if t in page_text:
#                 # find element with that visible text
#                 elems = driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{t}')]")
#                 if elems:
#                     return elems[0]
#     except Exception:
#         pass

#     return None

# def _element_center_screen_coords(driver, element):
#     """Return (x_screen, y_screen) in physical pixels for clicking."""
#     try: # Ưu tiên CDP (Chrome DevTools Protocol) để lấy tọa độ chính xác
#         rect = driver.execute_script("return arguments[0].getBoundingClientRect();", element)
#         res = driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
#         win_id = res.get("windowId")
#         bounds = driver.execute_cdp_cmd("Browser.getWindowBounds", {"windowId": win_id})
#         left, top = bounds.get("left", 0), bounds.get("top", 0)
#         dpr = driver.execute_script("return window.devicePixelRatio || 1;")
#         center_x = left + (rect['left'] + rect['width'] / 2) * dpr
#         center_y = top + (rect['top'] + rect['height'] / 2) * dpr
#         return int(center_x), int(center_y)
#     except Exception: # Fallback nếu CDP thất bại
#         rect = driver.execute_script("return arguments[0].getBoundingClientRect();", element)
#         win_pos = driver.get_window_position()
#         outer = driver.execute_script("return window.outerHeight;")
#         inner = driver.execute_script("return window.innerHeight;")
#         chrome_offset = max(0, outer - inner)
#         dpr = driver.execute_script("return window.devicePixelRatio || 1;")
#         center_x = win_pos['x'] + (rect['left'] + rect['width'] / 2) * dpr
#         center_y = win_pos['y'] + chrome_offset * dpr + (rect['top'] + rect['height'] / 2) * dpr
#         return int(center_x), int(center_y)
    

# def _human_like_move(self, target_x, target_y=None, duration=1.0):
#     """Moves mouse smoothly with jitter to simulate human movement."""

#     if target_y is None:
#         print("[⚠️] _human_like_move: target_y bị thiếu, không thể di chuyển.")
#         target_y = target_x + 100

#     try:
#         start_x, start_y = pyautogui.position()
#         duration = max(0.2, duration)
#         steps = int(duration * 50)

#         for i in range(steps + 1):
#             t = i / steps
#             ease = 3 * t**2 - 2 * t**3
#             x = start_x + (target_x - start_x) * ease
#             y = start_y + (target_y - start_y) * ease
#             jitter_scale = (1 - abs(0.5 - t) * 2) * 2
#             x += random.uniform(-1.0, 1.0) * jitter_scale
#             y += random.uniform(-1.0, 1.0) * jitter_scale
#             pyautogui.moveTo(int(x), int(y), duration=0)
#             time.sleep(duration / steps)
#     except Exception as e:
#         print(f"[❌] _human_like_move: Gặp lỗi khi di chuyển chuột: {e}")


# class CloudflareSolver:
#     def __init__(self, driver: WebDriver, screenshot_dir="debug_captcha"):
#         self.driver = driver
#         self.screenshot_dir = screenshot_dir
#         os.makedirs(self.screenshot_dir, exist_ok=True)

#     def solve_captcha(self, dry_run_click=True) -> bool:
#         """
#         Main method to solve Cloudflare CAPTCHA.
#         First, it tries the reliable iframe switch method.
#         If that fails, it falls back to the robust human-like pyautogui method.

#         Args:
#             dry_run_click (bool): If True, the pyautogui method will only move the mouse but not click.
        
#         Returns:
#             bool: True if the CAPTCHA is likely solved or not present.
#         """
#         # --- ATTEMPT 1: Iframe Switch (Nhanh và ổn định) ---
#         logger.info("--- CAPTCHA Solver: Attempt 1 (Iframe Switch) ---")
#         try:
#             iframe = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, '//iframe[contains(@src, "challenges.cloudflare.com")]')))
#             self.driver.switch_to.frame(iframe)
#             checkbox = WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="checkbox"], #checkbox')))
#             checkbox.click()
#             self.driver.switch_to.default_content()
#             time.sleep(5)
#             if not self._is_captcha_still_present():
#                 logger.info("SUCCESS: CAPTCHA solved via iframe switch method.")
#                 return True
#             else:
#                 logger.warning("Iframe switch click did not solve the CAPTCHA. Proceeding to fallback.")
#         except Exception:
#             logger.warning("Could not solve via iframe switch. Proceeding to fallback.")
#             try: self.driver.switch_to.default_content()
#             except Exception: pass

#         # --- ATTEMPT 2: PyAutoGUI Fallback (Mạnh mẽ) ---
#         logger.info("--- CAPTCHA Solver: Attempt 2 (Human-like Mouse Fallback) ---")
#         self._solve_with_pyautogui(dry_run=dry_run_click)
        
#         time.sleep(5)
#         if not self._is_captcha_still_present():
#             logger.info("SUCCESS: CAPTCHA likely solved via pyautogui fallback.")
#             return True
#         else:
#             logger.error("FAILURE: CAPTCHA is still present after all attempts.")
#             self.diagnose()
#             return False
        
#     def human_like_interaction(self, scroll_attempts=3, move_attempts=5):
#         """
#         Simulates human-like behavior on the current page to appear less like a bot.
#         This should be called right after a page load.
#         """
#         logger.info("--- Performing human-like interactions (scrolling and moving mouse)... ---")
#         try:
#             # 1. Cuộn trang ngẫu nhiên
#             page_height = self.driver.execute_script("return document.body.scrollHeight")
#             for _ in range(random.randint(1, scroll_attempts)):
#                 scroll_to = random.randint(0, int(page_height * 0.7))
#                 self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
#                 time.sleep(random.uniform(0.5, 1.5))
            
#             # 2. Di chuyển chuột đến các vị trí ngẫu nhiên trên màn hình
#             viewport_size = self.driver.get_window_size()
#             for _ in range(random.randint(2, move_attempts)):
#                 target_x = random.randint(int(viewport_size['width'] * 0.1), int(viewport_size['width'] * 0.9))
#                 target_y = random.randint(int(viewport_size['height'] * 0.1), int(viewport_size['height'] * 0.9))
#                 _human_like_move(target_x, target_y, duration=random.uniform(0.5, 1.0))
            
#             logger.info("--- Human-like interactions complete. ---")
#         except Exception as e:
#             logger.warning(f"An error occurred during human-like interaction simulation: {e}")

#     def _solve_with_pyautogui(self, dry_run=True, wait_for_host=3):
#         """Integrated flow from your provided code."""
#         try:
#             # Bring window to front (best-effort)
#             self.driver.switch_to.window(self.driver.current_window_handle)
#             time.sleep(0.2)
            
#             # Find host element
#             host = _find_challenge_host(self.driver)
#             if host is None and wait_for_host > 0:
#                 time.sleep(wait_for_host)
#                 host = _find_challenge_host(self.driver)

#             if host:
#                 rect = self.driver.execute_script("return arguments[0].getBoundingClientRect();", host)
#                 logger.info("Found host element rect: %s", rect)
#                 if rect.get('width', 0) > 0 and rect.get('height', 0) > 0:
#                     x_screen, y_screen = _element_center_screen_coords(self.driver, host)
#                     logger.info("Computed screen coords: %s, %s", x_screen, y_screen)
#                     if x_screen > 0 and y_screen > 0:
#                         logger.info("Moving mouse to target...")
#                         _human_like_move(x_screen, y_screen, duration=random.uniform(0.7, 1.2))
#                     else:
#                         logger.warning("Computed screen coords are invalid.")
#                 else: host = None # Treat as not found
            
#             if not host: # Fallback if host not found or has zero size
#                 logger.warning("No valid host found. Falling back to viewport top-center.")
#                 win_pos = self.driver.get_window_position()
#                 inner_w = self.driver.execute_script("return window.innerWidth;")
#                 inner_h = self.driver.execute_script("return window.innerHeight;")
#                 chrome_offset = self.driver.execute_script("return window.outerHeight - window.innerHeight;") or 110
#                 x_screen = win_pos['x'] + inner_w / 2
#                 y_screen = win_pos['y'] + chrome_offset + inner_h * 0.3
#                 if x_screen > 0 and y_screen > 0:
#                     logger.info(f"Moving mouse to fallback coords: X={int(x_screen)}, Y={int(y_screen)}")
#                     _human_like_move(int(x_screen), int(y_screen), duration=random.uniform(0.8, 1.4))
#                 else:
#                     logger.error("Fallback screen coords are invalid. Aborting.")
#                     return False
            
#             if dry_run:
#                 logger.warning("Dry-run is enabled, NOT clicking.")
#                 return True
            
#             logger.info("Executing click...")
#             pyautogui.click()
#             return True

#         except Exception as e:
#             logger.exception("Unhandled error in _solve_with_pyautogui: %s", e)
#             return False
        

#     def _solve_with_human_like_mouse(self, dry_run=True, wait_if_not_found=4) -> bool:
#         """
#         The "berserker" mode. Tries to find the CAPTCHA and WILL move the mouse to a
#         target location for observation, even if it fails to find a specific element.
#         """
#         try:
#             # Bring browser window to front
#             self.driver.switch_to.window(self.driver.current_window_handle)
#             time.sleep(0.5)

#             # 1. Find iframe
#             iframe = next((f for f in self.driver.find_elements(By.TAG_NAME, "iframe")
#                         if "challenges.cloudflare.com" in (f.get_attribute("src") or "")), None)

#             target_x, target_y = None, None

#             if iframe:
#                 logger.info("Pyautogui Fallback: Found iframe. Calculating calibrated coordinates...")
#                 rect = self.driver.execute_script("return arguments[0].getBoundingClientRect();", iframe)
                
#                 if rect and rect.get("width", 0) > 0:
#                     window_pos = self.driver.get_window_position()
#                     chrome_offset = self.driver.execute_script("return window.outerHeight - window.innerHeight;") or 110
                    
#                     # === PHẦN HIỆU CHỈNH TỌA ĐỘ ===
                    
#                     # Tọa độ X: Không lấy tâm (width / 2) nữa, mà lấy 1/4 từ bên trái
#                     # Điều này sẽ dịch con trỏ sang trái
#                     x_offset_ratio = 0.12 # 1/4 từ trái sang
#                     target_x = window_pos['x'] + rect['left'] + (rect['width'] * x_offset_ratio)

#                     # Tọa độ Y: Lấy tâm (height / 2) và cộng thêm một chút để dịch xuống
#                     # Nếu chia thành 9 phần, lùi xuống 1 phần nghĩa là vị trí khoảng 5/9 hoặc 6/9 từ trên xuống
#                     y_offset_ratio = 0.44 # 60% từ trên xuống
#                     target_y = window_pos['y'] + chrome_offset + rect['top'] + (rect['height'] * y_offset_ratio)
                    
#                     # ===============================

#                     logger.info(f"Targeting CALIBRATED coordinates: X={int(target_x)}, Y={int(target_y)}")
#                 else:
#                     iframe = None # Coi như không tìm thấy nếu kích thước không hợp lệ
            
#             if not iframe: # Last resort fallback (nếu không tìm thấy iframe)
#                 logger.warning("Pyautogui Fallback: Iframe not found. Moving to viewport top-left as last resort.")
#                 window_pos = self.driver.get_window_position()
#                 inner_w = self.driver.execute_script("return window.innerWidth;")
#                 inner_h = self.driver.execute_script("return window.innerHeight;")
#                 chrome_offset = self.driver.execute_script("return window.outerHeight - window.innerHeight;") or 110
                
#                 # Thay vì tâm, mình nhắm vào vùng trên-bên-trái
#                 target_x = window_pos['x'] + inner_w * 0.25
#                 target_y = window_pos['y'] + chrome_offset + inner_h * 0.3
#                 logger.info(f"Targeting last resort coordinates: X={int(target_x)}, Y={int(target_y)}")
            
#             if target_x is None or target_y is None:
#                 logger.error("Could not determine target coordinates. Aborting.")
#                 return False
#             # Move the mouse
#             logger.info("Executing human-like mouse movement...")
#             _human_like_move(target_x + random.randint(-15, 15), target_y + random.randint(-15, 15), duration=random.uniform(0.4, 0.8))
#             _human_like_move(target_x, target_y, duration=random.uniform(0.3, 0.7))
#             logger.info("Mouse movement complete.")

#             if dry_run:
#                 logger.warning("Dry-run is enabled, NOT clicking.")
#                 return True
            
#             logger.info("Executing click...")
#             pyautogui.click()
#             return True

#         except Exception as e:
#             logger.exception(f"Unhandled error in _solve_with_human_like_mouse: {e}")
#             return False
            
#     def _is_captcha_still_present(self, timeout: int = 3) -> bool:
#         """Checks if the CAPTCHA iframe is still on the page."""
#         return len(self.driver.find_elements(By.XPATH, '//iframe[contains(@src, "challenges.cloudflare.com")]')) > 0

#     def diagnose(self):
#         """
#         Collects comprehensive diagnostic information when a CAPTCHA
#         is detected and potentially failing to be solved.
#         """
#         ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
#         debug_info = {
#             "timestamp_utc": datetime.utcnow().isoformat(),
#             "page_url": self.driver.current_url,
#             "diagnostics": {}
#         }

#         try:
#             # 1. Chụp ảnh màn hình
#             full_path = os.path.join(self.screenshot_dir, f"diagnosis_{ts}.png")
#             self.driver.save_screenshot(full_path)
#             debug_info['diagnostics']['screenshot'] = full_path
#             logger.info(f"Saved diagnostic screenshot to: {full_path}")
            
#             # 2. Lấy thông tin Navigator
#             ua = self.driver.execute_script("return navigator.userAgent")
#             platform = self.driver.execute_script("return navigator.platform")
#             webdriver_flag = self.driver.execute_script("return navigator.webdriver")
#             debug_info['diagnostics']['navigator'] = {
#                 "userAgent": ua, 
#                 "platform": platform, 
#                 "webdriver_flag": webdriver_flag
#             }
            
#             # 3. Lấy Cookies
#             debug_info['diagnostics']['cookies'] = self.driver.get_cookies()
            
#             # 4. Lấy HTML của trang
#             debug_info['diagnostics']['page_source_snippet'] = self.driver.page_source[:5000]

#         except Exception as e:
#             logger.error(f"Could not complete diagnostics: {e}")
        
#         # Lưu file JSON
#         json_path = os.path.join(self.screenshot_dir, f"diagnosis_{ts}.json")
#         try:
#             with open(json_path, "w", encoding="utf-8") as f:
#                 json.dump(debug_info, f, indent=2, ensure_ascii=False)
#             logger.info(f"Saved diagnostic JSON to: {json_path}")
#         except Exception as e:
#             logger.error(f"Could not save diagnostic JSON: {e}")
            
#         return debug_info

# File: scrapers/utils/cloudflare_solver.py

import os
import time
import json
import logging
import random
import math
from datetime import datetime
import pyautogui

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

logger = logging.getLogger(__name__)
pyautogui.FAILSAFE = True

# ====================================================================
# === CÁC HÀM HELPER DI CHUYỂN CHUỘT SIÊU THỰC (TỪ CODE CỦA MÀY) ===
# ====================================================================
def _bezier_point(t, p0, p1, p2, p3):
    u = 1 - t
    return ((u**3)*p0[0] + 3*(u**2)*t*p1[0] + 3*u*(t**2)*p2[0] + (t**3)*p3[0],
            (u**3)*p0[1] + 3*(u**2)*t*p1[1] + 3*u*(t**2)*p2[1] + (t**3)*p3[1])

def _unit_vector(dx, dy):
    d = math.hypot(dx, dy)
    return (dx / d, dy / d) if d != 0 else (1.0, 0.0)

def _perp_vector(vx, vy):
    return (-vy, vx)

def human_like_move(target_x, target_y, duration=1.0, overshoot=True):
    start_x, start_y = pyautogui.position()
    dx, dy = target_x - start_x, target_y - start_y
    dist = math.hypot(dx, dy)
    
    # Logic overshoot
    if overshoot and dist > 15:
        ux, uy = _unit_vector(dx, dy)
        ov = random.uniform(8, 18)
        overshoot_pt = (target_x + ux * ov, target_y + uy * ov)
        main_end = overshoot_pt
        back_duration = max(0.1, duration * random.uniform(0.1, 0.2))
        main_duration = max(0.2, duration - back_duration)
    else:
        main_end, main_duration, back_duration = (target_x, target_y), duration, 0.0
        
    # Đường cong Bezier
    p0, p3 = (start_x, start_y), main_end
    dir_x, dir_y = p3[0] - p0[0], p3[1] - p0[1]
    cdist = max(25, dist * 0.3)
    perp = _perp_vector(dir_x, dir_y)
    pvx, pvy = _unit_vector(perp[0], perp[1])
    amp = random.uniform(0.2, 0.6) * cdist * random.choice([-1, 1])
    p1 = (p0[0] + dir_x * 0.25 + pvx * amp, p0[1] + dir_y * 0.25 + pvy * amp)
    p2 = (p0[0] + dir_x * 0.75 - pvx * amp * 0.5, p0[1] + dir_y * 0.75 - pvy * amp * 0.5)

    # Di chuyển chính
    steps = max(15, int(main_duration * 60))
    for i in range(steps + 1):
        t = i / steps
        ease = 3*t**2 - 2*t**3
        bx, by = _bezier_point(ease, p0, p1, p2, p3)
        jitter = max(1.8, (1 - t**1.5) * 2.5)
        pyautogui.moveTo(bx + random.uniform(-jitter, jitter), by + random.uniform(-jitter, jitter), duration=0)
        time.sleep(main_duration / steps)
        
    # Di chuyển lùi (nếu có)
    if back_duration > 0:
        human_like_move(target_x, target_y, duration=back_duration, overshoot=False)

# ====================================================================
# === LỚP SOLVER CHÍNH - ĐƯỢC NÂNG CẤP ===
# ====================================================================
class CloudflareSolver:
    def __init__(self, driver: WebDriver, screenshot_dir="debug_captcha"):
        self.driver = driver
        self.screenshot_dir = screenshot_dir
        os.makedirs(self.screenshot_dir, exist_ok=True)

    def solve_captcha(self, dry_run_click=True) -> bool:
        logger.info("--- Cloudflare Solver Activated ---")
        try:
            # Bước 1: Đưa cửa sổ ra tiền tuyến và chờ đợi
            self.driver.switch_to.window(self.driver.current_window_handle)
            time.sleep(random.uniform(1, 2))
            
            # Bước 2: Tìm mục tiêu (iframe)
            host = self._find_challenge_host()
            if not host:
                logger.info("No CAPTCHA host found. Assuming page is clear.")
                return True

            # Bước 3: Ngắm bắn (tính toán tọa độ)
            coords = self._get_element_screen_coords(host)
            if not coords:
                logger.error("Could not calculate screen coordinates for CAPTCHA host.")
                self.diagnose()
                return False
            
            target_x, target_y = coords
            
            # Bước 4: Bóp cò (di chuyển và click)
            logger.info(f"Moving mouse to target: ({target_x}, {target_y})")
            human_like_move(target_x, target_y, duration=random.uniform(0.8, 1.5))
            
            if dry_run_click:
                logger.warning("Dry-run enabled. SKIPPING CLICK.")
            else:
                logger.info("Executing click.")
                pyautogui.click()
            
            time.sleep(5)
            
            # Bước 5: Kiểm tra kết quả
            if self._is_captcha_still_present():
                logger.error("FAILURE: CAPTCHA is still present after attempt.")
                self.diagnose()
                return False
            else:
                logger.info("SUCCESS: CAPTCHA has disappeared after attempt.")
                return True

        except Exception as e:
            logger.exception(f"An unexpected error occurred in solve_captcha: {e}")
            self.diagnose()
            return False

    def _find_challenge_host(self):
        """Finds the most likely CAPTCHA host element."""
        try:
            # Ưu tiên tìm iframe trước
            iframes = self.driver.find_elements(By.XPATH, '//iframe[contains(@src, "challenges.cloudflare.com")]')
            if iframes:
                logger.info("Found CAPTCHA host: iframe")
                return iframes[0]
        except Exception: pass
        
        logger.warning("Could not find CAPTCHA iframe, trying text-based search...")
        # Fallback tìm theo text
        try:
            texts = ["verify you are human", "xác minh bạn là con người"]
            for t in texts:
                elems = self.driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{t}')]")
                if elems:
                    logger.info(f"Found CAPTCHA host: element with text '{t}'")
                    return elems[0]
        except Exception: pass
        
        return None

    def _get_element_screen_coords(self, element):
        """Calculates the screen coordinates of an element's center."""
        try:
            rect = self.driver.execute_script("return arguments[0].getBoundingClientRect();", element)
            if not (rect and rect.get('width', 0) > 0 and rect.get('height', 0) > 0):
                logger.warning("Host element has zero or invalid size.")
                return None
            
            win_pos = self.driver.get_window_position()
            chrome_offset = self.driver.execute_script("return window.outerHeight - window.innerHeight;") or 110
            
            center_x = win_pos['x'] + rect['left'] + rect['width'] / 2
            center_y = win_pos['y'] + chrome_offset + rect['top'] + rect['height'] / 2
            
            return int(center_x), int(center_y)
        except Exception as e:
            logger.error(f"Failed to get element screen coordinates: {e}")
            return None

    def _is_captcha_still_present(self, timeout: int = 3) -> bool:
        """Checks if the CAPTCHA is still on the page."""
        try:
            WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((By.XPATH, '//iframe[contains(@src, "challenges.cloudflare.com")]')))
            return True
        except TimeoutException:
            return False
            
    def diagnose(self):
        """
        Collects comprehensive diagnostic information when a CAPTCHA
        is detected and potentially failing to be solved.
        """
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        debug_info = {
            "timestamp_utc": datetime.utcnow().isoformat(),
            "page_url": self.driver.current_url,
            "diagnostics": {}
        }

        try:
            # 1. Chụp ảnh màn hình
            full_path = os.path.join(self.screenshot_dir, f"diagnosis_{ts}.png")
            self.driver.save_screenshot(full_path)
            debug_info['diagnostics']['screenshot'] = full_path
            logger.info(f"Saved diagnostic screenshot to: {full_path}")
            
            # 2. Lấy thông tin Navigator
            ua = self.driver.execute_script("return navigator.userAgent")
            platform = self.driver.execute_script("return navigator.platform")
            webdriver_flag = self.driver.execute_script("return navigator.webdriver")
            debug_info['diagnostics']['navigator'] = {
                "userAgent": ua, 
                "platform": platform, 
                "webdriver_flag": webdriver_flag
            }
            
            # 3. Lấy Cookies
            debug_info['diagnostics']['cookies'] = self.driver.get_cookies()
            
            # 4. Lấy HTML của trang
            debug_info['diagnostics']['page_source_snippet'] = self.driver.page_source[:5000]

        except Exception as e:
            logger.error(f"Could not complete diagnostics: {e}")
        
        # Lưu file JSON
        json_path = os.path.join(self.screenshot_dir, f"diagnosis_{ts}.json")
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(debug_info, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved diagnostic JSON to: {json_path}")
        except Exception as e:
            logger.error(f"Could not save diagnostic JSON: {e}")
            
        return debug_info