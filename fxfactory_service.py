import requests
import os
import time
import schedule
from openai import OpenAI
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from PIL import Image
import sys
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
        print(data)
        exit("Program dihentikan!")
        return data  # Mengembalikan data sentimen berita dalam format JSON
    except Exception as e:
        print(f"❌ Error mengambil data sentimen berita: {e}")
        return None

def read_forex_news(image_path):
    """🔹 Mengirim screenshot ke OpenAI API dan membaca kalender berita."""
    with open(image_path, "rb") as image_file:
        image_data = base64.b64encode(image_file.read()).decode("utf-8")

    json_filename = os.path.join(SAVE_DIR, f"forexfactory_{datetime.now().strftime('%Y-%m-%d')}.json")

    #example news
    news_titles = """
    "Asia And Europe Markets Mixed, Gold Jumps On Safe-Haven Demand - Global Markets Today While US Slept - SmartETFs Asia Pacific Dividend Builder ETF  ( ARCA:ADIV ) ",
    "Alamos Gold Stock Hits 52-Week High: What's Driving Its Performance?",
    "Gold Fields Stock Hits 52-Week High: What's Driving Its Performance?",
    "Banks and fintechs join 'stablecoin gold rush'",
    "'I'm deeply disturbed': My portfolio fell 20%. Do I sell my stocks and buy gold?",
    "South Korea's mint is grappling with a gold bar shortage as supply constraints bite",
    "Peter Schiff Predicts A Bitcoin Strategic Reserve Will 'Accelerate' Dollar's Demise: 'Ultimate Winner Will Be Gold'",
    "Will Gold Continue to Shine in 2025?",
    "Bitcoin no longer 'safe haven' as $82K BTC price dive leaves gold on top",
    "S&P 500 Vs US Dollar: While Equities Take The Tariff Punch, Greenback Sheds Tariff Fears: 'Both Can't Be Right,' Says Former Goldman FX Strategist - Invesco QQQ Trust, Series 1  ( NASDAQ:QQQ ) , SPDR S&P 500  ( ARCA:SPY ) ",
    "Asia Markets Mixed, Europe Advances, Gold Gains 1.2% - Global Markets Today While US Slept - SmartETFs Asia Pacific Dividend Builder ETF  ( ARCA:ADIV ) ",
    "Robert Kiyosaki Forecasts Global Financial Meltdown, Recommends Bitcoin As Safe Haven: 'Buy Bitcoin, Gold and Silver'",
    "Newcore Gold Increases Drill Program to 35,000 Metres and Intersects 1.28 g/t Gold over 63.0 Metres at the Enchi Gold Project, Ghana",
    "Newcore Gold Announces Closing of $15 Million Private Placement Financing - Newcore Gold  ( OTC:NCAUF ) ",
    "Newcore Gold Announces Closing of $15 Million Private Placement Financing",
    "White Gold Corp. Encounters Broad Near Surface Gold-Bearing Structure with High Density Quartz Veining in All Holes on Newly Discovered 2.2 km+ Chris Creek Gold Target - White Gold  ( OTC:WHGOF ) ",
    "White Gold Corp. Encounters Broad Near Surface Gold-Bearing Structure with High Density Quartz Veining in All Holes on Newly Discovered 2.2 km+ Chris Creek Gold Target",
    "The Zacks Analyst Blog Highlights Franklin Responsibly Sourced Gold ETF, VanEck Merk Gold Trust, GraniteShares Gold Trust, iShares Gold Trust and iShares Gold Trust Micro",
    "5 ETFs Riding on Gold's Longest Rally in Four Years",
    "Asia Markets Dip While Europe Advance, Gold Retreats - Global Markets Today While US Slept - SmartETFs Asia Pacific Dividend Builder ETF  ( ARCA:ADIV ) ",
    "China's nod for insurers to buy gold may drive prices above US$3,000: analysts",
    "3 Best Gold Plays for a Volatile 2025",
    "Gold Rush 2.0? Here's How To Play The New Bull Market - Anglogold Ashanti  ( NYSE:AU ) , iShares MSCI Global Gold Miners ETF  ( NASDAQ:RING ) , Gold Fields  ( NYSE:GFI ) ",
    "Gold Mining ETF  ( GDXJ )  Hits New 52-Week High",
    "Endeavour Silver Continues to Intersect High-Grade Silver-Gold Mineralization at its Bola\u00f1itos Operation",
    "Eldorado Gold Delivers Strong 2024 Full Year and Fourth Quarter Financial and Operational Results; Positive Free Cash Flow Realized in the Quarter and Full Year",
    "Osisko Declares First Quarter 2025 Dividend - Osisko Gold Royalties  ( NYSE:OR ) ",
    "Alamos Gold Reports Fourth Quarter and Year-End 2024 Results",
    "Alamos Gold Reports Fourth Quarter and Year-End 2024 Results - Alamos Gold  ( NYSE:AGI ) ",
    "Newcore Gold Recognized as a 2025 TSX Venture 50 Company",
    "Red Pine Successfully Expands Gold Mineralization Beyond 2024 Mineral Resource Estimate and Discovers Parallel Shoot at Depth",
    "Alamos Gold Reports Mineral Reserves and Resources for the Year-Ended 2024",
    "Asia And Europe Markets Mixed, Gold Advances - Global Markets Today While US Slept - SmartETFs Asia Pacific Dividend Builder ETF  ( ARCA:ADIV ) ",
    "Opinion | From oil to bitcoin, Trump envisions multipronged US 'gold standard'",
    "Alamos Gold Announces Development Plan for High-Return Burnt Timber and Linkwood Satellite Deposits",
    "Inflation Rises More Than Expected In January, Chills Interest Rate Cut Hopes - SPDR Gold Trust  ( ARCA:GLD ) ",
    "Asia Ex-India And Europe Markets Advance, Gold Retreats From All Time High - Global Markets Today While US Slept - SmartETFs Asia Pacific Dividend Builder ETF  ( ARCA:ADIV ) ",
    "The Zacks Analyst Blog Barrick Gold and Kinross Gold",
    "2 Gold Mining Stocks That Could Explode Under Trump's Tariffs",
    "Newcore Gold Announces Upsize of Private Placement Financing to $15 Million",
    "Newcore Gold Announces $12 Million Brokered Private Placement Financing",
    "Gold Prices Near $3000, Outpacing US Equities As It Hits A Fresh Record High Amid Trump's Trade War-Led Market Uncertainty - Invesco QQQ Trust, Series 1  ( NASDAQ:QQQ ) , SPDR S&P 500  ( ARCA:SPY ) ",
    "Robert Kiyosaki Advocates for Gold and Bitcoin Over Dollar Savings: 'It Is Smarter and Safer'",
    "US Economy Adds Fewer Than Expected Jobs In January, Unemployment Rate Slows, Wage Growth Spikes - SPDR S&P 500  ( ARCA:SPY ) , SPDR Gold Trust  ( ARCA:GLD ) ",
    "Gold Rally to Continue: Leveraged ETFs to Make Profits",
    "Eldorado Gold Provides Skouries Project Update; 2025 Detailed Company Production & Cost Guidance; Updated Three-Year Growth Profile; Conference Call Details",
    "Eldorado Gold Provides Skouries Project Update; 2025 Detailed Company Production & Cost Guidance; Updated Three-Year Growth Profile; Conference Call Details - Eldorado Gold  ( NYSE:EGO ) ",
    "Gold Extends Records As Central Banks Rush To Buy Bullion, Miners Eye Sixth Straight Winning Week - Alamos Gold  ( NYSE:AGI ) , Aris Mining  ( AMEX:ARMN ) ",
    "Gold ETFs Soar to New Highs on Tariff Turmoil: What's Ahead?",
    "5 Reasons Why Gold ETFs Are Smart Bets Now",
    "4 Top Gold Stocks to Buy With Investors Seeking Safe Haven",
    "Red Pine Drilling Expands Gold System at Wawa Gold Project",
    "Stocks Trim Losses On Mexico Tariff Delay, Gold Extends Records, Tesla Sinks 6%: What's Driving Markets Monday? - Apple  ( NASDAQ:AAPL ) ",
    "EnviroGold Global Highlights Resilience to U.S. Government Tariffs and Benefits from Stronger U.S. Dollar",
    "Gold Breaks Out, Disturbing iPhone Decline, High Demand For Nvidia RTX 50, Amazon Offers DeepSeek",
    "Fed's Favorite Inflation Gauge Rises As Predicted, Consumer Spending Jumps At Fastest Pace In 9 Months - Invesco QQQ Trust, Series 1  ( NASDAQ:QQQ ) , SPDR Gold Trust  ( ARCA:GLD ) ",
    "Wall Street Remains Flat, Microsoft Eyes Heaviest Drop Since Late 2022, Gold Sets Fresh Records: What's Driving Markets Thursday? - Apple  ( NASDAQ:AAPL ) ",
    "Newcore Gold Drilling Intersects 1.85 g/t Gold over 62.0 Metres and 0.75 g/t Gold over 68.0 Metres at the Enchi Gold Project, Ghana - Newcore Gold  ( OTC:NCAUF ) ",
    "Newcore Gold Drilling Intersects 1.85 g/t Gold over 62.0 Metres and 0.75 g/t Gold over 68.0 Metres at the Enchi Gold Project, Ghana",
    "Ray Dalio Owns Bitcoin For Diversification, But Prefers Gold As The 'Purest Play' For Store of Value",
    "Fed Holds Interest Rates Steady, Halts Streak Of Consecutive Cuts, Says Inflation Remains 'Elevated' - SPDR Gold Trust  ( ARCA:GLD ) ",
    "Robinhood Expands Trading Services With Bitcoin, Oil, Gold Futures - Robinhood Markets  ( NASDAQ:HOOD ) ",
    "Alamos Gold Announces Receipt of Environmental Permit Amendment Allowing for the Start of Construction on the Puerto Del Aire Project in Mexico"""
  
    prompt = """
    Extract all economic news events from this screenshot of the calendar and convert each into a structured JSON document. 
    For each event in the table, provide a comprehensive analysis with historical context and a data-driven forecast using global economic sources. 
    Ensure you include all events and structure each as follows:
    {
    "timestamp": "{timestamp}",
    "source": "{source}",
    "event":[{
        "currency": "{currency}",
        "event_name": "{event_name}",
        "event_date": "{date_time}",
        "event_status": "{status}",
        "impact_level": "{level}",
        "forecast": {forecast},
        "actual": {actual},
        "previous": {previous}
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
    """

    #prompt += "You can use this news to get sentiment in prediction." + news_titles


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
        #tools=tools,
        #tool_choice="auto",
        response_format={"type": "json_object"},
        temperature=0.75,
        max_tokens=16384
    )

    #print(response)
    #sys.exit("Program dihentikan!")  # Bisa juga dengan sys.exit(1) untuk error

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
            if (
                os.path.isfile(file_path)
                and file.lower().endswith((".png", ".jpg", ".jpeg"))
                and file.startswith("forexfactory")  # Cek prefix
            ):
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
schedule.every(15).minutes.do(take_screenshot)
print("🚀 Service berjalan... Ambil full-page screenshot setiap 15 menit.")
take_screenshot()

while True:
    schedule.run_pending()
    time.sleep(1)