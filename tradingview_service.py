import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import tempfile
import base64
from openai import OpenAI

# 🔹 Ambil API Key dari environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("❌ ERROR: API Key OpenAI tidak ditemukan. Setel variabel lingkungan OPENAI_API_KEY.")

# 🔹 Inisialisasi klien OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# List user-agent yang berbeda
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
]
random_user_agent = random.choice(user_agents)

# Konfigurasi Chrome
options = Options()
options.add_argument(f"user-agent={random_user_agent}")
options.add_argument("--headless=new")  # Mode headless

# Gunakan direktori sesi sementara agar tidak mudah terdeteksi
#user_data_dir = tempfile.mkdtemp()
#options.add_argument(f"--user-data-dir={user_data_dir}")
options.add_argument("--user-data-dir=/tmp/chrome_dev_test")  # Gunakan sesi login yang sudah ada

options.add_argument("--no-sandbox")  
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

# Jalankan ChromeDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Atur ukuran layar ke 1200x1200
driver.set_window_size(1200, 1200)

# URL Halaman Chart TradingView
chart_url = "https://www.tradingview.com/chart/90FNc4nG/?symbol=OANDA%3AXAUUSD"
driver.get(chart_url)

# Sembunyikan webdriver agar tidak terdeteksi
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

print("✅ Chrome berhasil dibuka. Mengecek apakah login diperlukan...")

try:
    # Cek apakah sudah login dengan mencari elemen utama chart
    if WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-container"))):
        print("🎉 Sudah login! Langsung ke halaman chart.")
    else:
        raise Exception("Elemen chart tidak ditemukan, mungkin perlu login.")
    
except Exception:
    print("🔄 Belum login, mencoba proses login...")
    
    try:
        # Cek tombol "log in"
        login_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'log in')]"))
        )
        login_button.click()
        print("✅ Tombol login diklik. Menunggu halaman login terbuka...")

        try:
            email_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Email')]"))
            )
            
            # Scroll ke tombol jika diperlukan
            driver.execute_script("arguments[0].scrollIntoView();", email_button)
            
            # Klik tombol email
            email_button.click()
            print("✅ Tombol Email berhasil diklik!")
        except Exception as e:
            print(f"❌ Gagal mengklik tombol Email: {e}")        
        
        # Tunggu input username muncul
        username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "id_username"))
        )
    
        # Kosongkan field username
        username_input.send_keys(Keys.CONTROL + "a")  # Select all text
        username_input.send_keys(Keys.DELETE)
        time.sleep(5)  # Tunggu agar benar-benar kosong
    
        # Masukkan email
        email_kamu = "mkemalw@gmail.com"  # GANTI dengan email kamu
        username_input.send_keys(email_kamu)
        print(f"✅ Berhasil input email: {email_kamu}")
    
        # Tunggu input password muncul
        password_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "id_password"))
        )
    
        # Kosongkan field password
        password_input.send_keys(Keys.CONTROL + "a")
        password_input.send_keys(Keys.DELETE)
        time.sleep(2)
    
        # Masukkan password
        password_kamu = "3Desember1986!@#"  # GANTI dengan password kamu
        password_input.send_keys(password_kamu)
        print("✅ Berhasil input password.")
    
        # Tunggu elemen tombol "Sign in" muncul dan bisa diklik
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@data-overflow-tooltip-text='Sign in']"))
        )
    
        # Scroll ke tombol jika tidak terlihat
        driver.execute_script("arguments[0].scrollIntoView();", login_button)
        time.sleep(1)
    
        # Klik tombol menggunakan JavaScript jika klik biasa gagal
        try:
            login_button.click()
            print("✅ Tombol Sign in diklik.")
        except:
            driver.execute_script("arguments[0].click();", login_button)
            print("✅ Tombol Sign in diklik via JavaScript.")

        # Tunggu redirect ke halaman chart
        time.sleep(5)
        
        #if "chart" not in driver.current_url:
        #    print("🔄 Tidak kembali ke chart. Reload halaman chart...")
        #    driver.get(chart_url)

        # Tunggu chart muncul
        #WebDriverWait(driver, 10).until(
        #    EC.presence_of_element_located((By.CLASS_NAME, "chart-container"))
        #)
        
        print("🎉 Login sukses! Kembali ke halaman chart XAU/USD.")

    except Exception as e:
        print(f"❌ Error saat login: {e}")
        driver.quit()
        exit()

# Buat folder screenshot jika belum ada
screenshot_folder = "screenshots"
os.makedirs(screenshot_folder, exist_ok=True)

def capture_screenshot():
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "chart-container"))  
        )
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(screenshot_folder, f"tradingview_XAUUSD_15M_{timestamp}.png")

        driver.save_screenshot(filename)
        print(f"✅ Screenshot saved: {filename}")

        return filename  # Kembalikan path gambar untuk digunakan dalam OCR

    except Exception as e:
        print(f"⚠️ Gagal mengambil screenshot: {e}")
        return None


def image_to_base64(image_path):
    if not os.path.exists(image_path):
        print(f"⚠️ File tidak ditemukan: {image_path}")
        return None
    
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def analyze_chart_with_gpt4o(image_base64):
    if image_base64 is None:
        print("⚠️ Tidak ada gambar untuk dianalisis.")
        return None
    
    try:
        prompt = """
        attached is screenshot from tradingview.
        provide the supply and demand area from the exponential move. 
        identified rally base rally, rally base drop, drop base drop, drop base rally and also consolidation move to identified the  area.
        please be aware in fake breakout and clear highest high and lowest low.
        {
        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S")
        "pair": "{pair}",
        "timeframe: "{timeframe}"
        "price_action":[{
            "demand_sup_area": nearest demand area in range (price - price),
            "supply_res_area": nearest supply area in price range (price - price)
        }]
        "recomendation":[{
            "bias" : "bullish/bearish/sideway"
            "RR" : Risk Reward Ratio (1:1.5/1:2/1:3/1:4)
            "action" : "wait/buylimit/buystop/selllimit/sellstop",
            "entry_price" : best entry price (numbers),
            "TP" : "",
            "SL" : "",
            "probability" : "0 - 100"
            "rationale" : explained in simple term why we take the action
            "prob_method" : Explain how AI calculate the probability
            "lot_size" : using kelly critetion in 1000 USD equity
        }]
        output are json to be consume on EA.
        """
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert in financial market analysis. Extract data from the image."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}", "detail": "high"}}
                    ]
                }],
            response_format={"type": "json_object"},
            temperature=0.75,
            max_tokens=16384
        )
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"⚠️ Gagal menganalisis gambar dengan GPT-4o: {e}")
        return None


# Loop untuk mengambil screenshot setiap 1 menit
try:
    while True:
        screenshot_path = capture_screenshot()
        if screenshot_path:
            image_base64 = image_to_base64(screenshot_path)
            analysis_result = analyze_chart_with_gpt4o(image_base64)
            print("📊 Analisis GPT-4o:", analysis_result)

        delay = random.randint(180, 200)
        print(f"⏳ Menunggu {delay} detik sebelum screenshot berikutnya...")
        time.sleep(delay)

except KeyboardInterrupt:
    print("\n🛑 Dihentikan oleh pengguna.")