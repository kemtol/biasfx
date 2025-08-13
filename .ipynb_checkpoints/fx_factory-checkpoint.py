import os
import time
import schedule
from openai import OpenAI
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from PIL import Image
import json

# 🔹 Ambil API Key dari environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("❌ ERROR: API Key OpenAI tidak ditemukan. Setel variabel lingkungan OPENAI_API_KEY.")

# 🔹 Inisialisasi klien OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# 🔹 Konfigurasi Chrome agar berjalan headless
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")

# 🔹 Folder penyimpanan screenshot
SAVE_DIR = "screenshots"
os.makedirs(SAVE_DIR, exist_ok=True)

def get_forex_news_sentiment():
    """🔹 Mengambil sentimen berita Forex dari Alpha Vantage API"""
    API_KEY = "W0CY87H193QOH05M"  # Ganti dengan API key Anda
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=FOREX:USD&apikey={API_KEY}"
    
    try:
        response = requests.get(url)
        data = response.json()
        return data  # Mengembalikan data sentimen berita dalam format JSON
    except Exception as e:
        print(f"❌ Error mengambil data sentimen berita: {e}")
        return None

def read_forex_news(image_path):
    """🔹 Mengirim screenshot ke OpenAI API dan membaca kalender berita."""
    with open(image_path, "rb") as image_file:
        image_data = base64.b64encode(image_file.read()).decode("utf-8")

    json_filename = os.path.join(SAVE_DIR, f"forexfactory_{datetime.now().strftime('%Y-%m-%d')}.json")

    prompt = """
    Extract all economic news events from this screenshot of the calendar and convert each into a structured JSON document. 
    For each event in the table, 
    provide a comprehensive analysis with historical context and a data-driven forecast using global economic sources. 
    Ensure you include all events and structure each as follows:
    {
    "timestamp": "{response_timestamp}",
    "source": "{source}",
    "event":[{
        "dxy_weekly":"bearish/bullish/sideway"
        "currency": "{currency}",
        "event_name": "{event_name}",
        "event_date": "{date_time}",
        "event_status": "{status}",
        "impact_level": "{level}",
        "forecast": {forecast},
        "actual": {actual},
        "last_forecast": {last_event_forecast},
        "last_actual": {last_event_actual},
        "ai":[{
            "ai_forecast": {ai_forecast},
            "ai_forecast_confidence": "0 to 100",
            "ai_pair_to_trade : {FX_PAIR}
            "ai_recommendation": "bullish/bearish",
            "ai_recommendation_position_timing": "putbeforenews/notrade/putafternews",
            "ai_rationale": "macro explanation in simple term"
            }]
        }
    }]

    Ensure that you capture, process and return data for each event separately,
    You can call function news sentiment to get more precise in prediction.
    """

    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_forex_news_sentiment",
                "description": "Mengambil data sentimen berita terbaru untuk analisis AI dalam menentukan bias pasar.",
                "parameters": {},
            }
        }
    ]

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an expert in financial market analysis. Extract data from the image."},
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}", "detail": "high"}}
            ]}],
        tools=tools,
        tool_choice="auto",
        response_format={"type": "json_object"},
        temperature=0.75,
        max_tokens=16384
    )

    try:
        # Ambil data baru dari API
        new_data = json.loads(response.choices[0].message.content)
        response_content = response.choices[0].message.content

        if not response_content:
            print("❌ Error: Respon dari OpenAI kosong atau None.")
            return None
        
        try:
            new_data = json.loads(response_content)
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding JSON dari OpenAI: {e}")
            return None
        
        # Pastikan data baru adalah sebuah list
        if isinstance(new_data, dict) and "events" in new_data:
            new_events = new_data["events"]
        elif isinstance(new_data, list):
            new_events = new_data
        else:
            # Jika tidak, bungkus dalam list
            new_events = [new_data]
    except json.JSONDecodeError:
        print("❌ Error: Respon dari API tidak valid.")
        return None

    # Membaca file JSON jika ada, dan menambah data baru
    if os.path.exists(json_filename):
        with open(json_filename, "r") as json_file:
            try:
                data = json.load(json_file)
                # Validasi jika data bukan list
                if not isinstance(data, list):
                    data = []
            except json.JSONDecodeError:
                data = []
    else:
        data = []

    # Tambahkan data baru ke dalam list existing
    data.extend(new_events)

    # Menulis data gabungan ke file JSON
    with open(json_filename, "w") as json_file:
        json.dump(data, json_file, indent=2)
    print(f"📄 Data Forex News diperbarui: {json_filename}")
    return data

def clear_old_screenshots():
    """🔹 Hapus hanya file gambar di folder screenshots sebelum mengambil yang baru."""
    for file in os.listdir(SAVE_DIR):
        file_path = os.path.join(SAVE_DIR, file)
        try:
            if os.path.isfile(file_path) and file.lower().endswith((".png", ".jpg", ".jpeg")):
                os.remove(file_path)
                print(f"🗑️ File gambar lama dihapus: {file_path}")
        except Exception as e:
            print(f"❌ Gagal menghapus {file_path}: {e}")

# Fungsi untuk mengambil screenshot dengan ukuran yang diinginkan
def take_screenshot():
    """🔹 Mengambil full-page screenshot ForexFactory dan parsing dengan OpenAI."""
    print("\n📸 Mengambil full-page screenshot forexfactory.com v0.0.6")
    # 🔥 Hapus screenshot lama sebelum mengambil yang baru
    clear_old_screenshots()

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1200, 960)  # Atur ukuran window sesuai keinginan
    driver.get("https://www.forexfactory.com/calendar?day=today")
    time.sleep(5)

    # **Mengatur tinggi window agar sesuai tinggi halaman**
    #page_height = driver.execute_script("return document.body.scrollHeight")
    driver.set_window_size(1200, 960)  # Set ukuran jendela ke dimensi desktop

    today_str = datetime.now().strftime("%Y-%m-%d")
    temp_png = os.path.join(SAVE_DIR, f"forexfactory_{today_str}.png")
    final_jpg = os.path.join(SAVE_DIR, f"forexfactory_{today_str}.jpg")

    # **Ambil screenshot**
    driver.save_screenshot(temp_png)
    driver.quit()
    print(f"✅ Full-page screenshot PNG tersimpan: {temp_png}")

    try:
        with Image.open(temp_png) as img:
            img.convert("RGB").save(final_jpg, "JPEG", quality=50)
        os.remove(temp_png)
        print(f"✅ Screenshot JPG tersimpan: {final_jpg}")

        # 🔥 **Parsing Kalender Forex menggunakan OpenAI**
        forex_data = read_forex_news(final_jpg)
        print("🔍 Hasil Parsing Forex News:\n")
        print(json.dumps(forex_data, indent=4))

    except Exception as e:
        print(f"❌ Gagal konversi PNG ke JPG: {e}")

# 🔹 Jadwalkan setiap 15 menit
schedule.every(1).minutes.do(take_screenshot)
print("🚀 Service berjalan... Ambil full-page screenshot setiap 15 menit.")
take_screenshot()

while True:
    schedule.run_pending()
    time.sleep(1)