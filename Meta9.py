import cloudscraper
import requests
import time
import re
import os
import sys
import pytz
import gc
import feedparser
import threading
import telebot
import html  
from datetime import datetime, timedelta, timezone
from queue import Queue
from bs4 import BeautifulSoup
from flask import Flask
from threading import Thread
from deep_translator import GoogleTranslator
from google import genai
from groq import Groq
from apscheduler.schedulers.background import BackgroundScheduler
from ecocal import Calendar 
import pandas as pd  
import psutil  
from pymongo import MongoClient  # 🎯 MongoDB Atlas కనెక్ట్ చేయడానికి యాడ్ చేశాం సార్

# --- SYSTEM ENCODING ---
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================================
# ⚙ CONFIGURATION & API KEYS
# ==========================================================
TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID") 
VIP_CHAT_ID = os.getenv("VIP_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")  # 🎯 మీ MongoDB URL రైల్వే నుండి ఆటోమేటిక్‌గా వస్తుంది

if not all([TOKEN, CHAT_ID, GEMINI_API_KEY, GROQ_API_KEY, VIP_CHAT_ID, MONGO_URI]):
    print("⚠ Warning: కొన్ని ముఖ్యమైన API Keys లేదా MONGO_URI సెట్ చేయబడలేదు!")

bot = telebot.TeleBot(TOKEN)
MODEL_NAME = "gemini-2.5-flash"

# --- టైమ్జోన్ సెటప్ ---
IST = pytz.timezone("Asia/Kolkata")
US = pytz.timezone("US/Eastern")
EU = pytz.timezone("Europe/Berlin")
JP = pytz.timezone("Asia/Tokyo")
HK = pytz.timezone("Asia/Hong_Kong")

# ==========================================================
# 🍃 MONGO DATABASE SETUP (డేటాబేస్ కనెక్షన్ లాజిక్)
# ==========================================================
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["MarketBotDB"]  # డేటాబేస్ పేరు
    
    # కలెక్షన్స్ (Collections)
    db_links = db["sent_links"]
    db_news = db["sent_news"]
    db_live_results = db["live_results"]
    print("✅ MongoDB Atlas తో కనెక్షన్ విజయవంతమైంది చంటి గారు!")
except Exception as e:
    print(f"❌ MongoDB కనెక్షన్ లోపం: {e}")

# ==========================================================
# 📊 DATA STORES & WATCHLISTS
# ==========================================================
rss_news_store = []
pinned_messages_store = []
sent_alerts = {}
sudden_move_sent = {}
gap_alert_sent = {}
collected_news = []
last_reset_date = datetime.now(IST).date()
economic_calendar_cache = {} 

MAX_NEWS = 1500  
CLEAR_COUNT = 500
ai_queue = Queue()
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

## 🔴 చంటి గారి 100% పక్కా మాస్టర్ వాచ్లిస్ట్
MY_WATCHLIST = [
    "ANANTRAJ", "ANANT RAJ", "APOLLO", "APOLLO HOSPITALS", "BBOX", "BLACK BOX", "ADANIENERGY", "ADANI ENERGY", "ADANI ENERGY SOLUTIONS", "ADANIPOWER", "ADANI POWER",
    "ATHER", "ATHER ENERGY", "ATHER ENERG", "ATHER EV", "ఏథర్", "Ather Energy Limited",
    "BHEL", "bhel", "భెల్", "bharat heavy electricals", "bharat heavy electricals limited",
    "BEL", "BHARAT ELECTRONICS", "BHARTIARTL", "BHARTI AIRTEL", "AIRTEL",
    "BLS", "BLS INTERNATIONAL", "BLUECLOUD", "BLUE CLOUD", "BSE", "BSE LTD",
    "CDSL", "CGPOWER", "CG POWER", "CHOLAFIN", "CHOLAMANDALAM", "CLEANMAX", "CLEAN MAX",
    "COFORGE", "DIXON", "DIXON TECH", "E2E", "E2E NETWORKS", "EIEL", "Enviro Infra Engineers Ltd",
    "ETERNAL", "FRACTAL", "GMDCLTD", "GMDC", "GOKEX", "GOKALDAS EXPORTS",
    "GROWW", "GRSE", "EMMVEE", "EMMVEE SOLAR", "EMMVEE PHOTOVOLTAIC",
    "HAL", "HINDUSTAN AERONAUTICS", "HDFCBANK", "HDFC BANK", "HINDCOPPER", "HINDUSTAN COPPER",
    "IDEA", "VODAFONE IDEA", "IDFCFIRSTB", "IDFC FIRST", "INDIGO", "INTERGLOBE AVIATION",
    "INFY", "INFOSYS", "INTERARCH", "ITC", "ITCHOTELS", "ITC HOTELS", "JKTYRE", "JK TYRE",
    "JSWSTEEL", "JSW STEEL", "KALAMANDIR", "SAI SILKS", "KALYANKJIL", "KALYAN JEWELLERS",
    "KAYNES", "KAYNES TECH", "KEC", "KEC INTERNATIONAL",
    "LEMONTREE", "LEMON TREE", "LENSKART", "LGEINDIA", "LG ELECTRONICS", "LT", "L&T",
    "LARSEN", "M&M", "MAHINDRA", "MAZDOCK", "MAZAGON DOCK", "MCX", "MEESHO",
    "NESTLEIND", "NESTLE", "NESTLE INDIA", "NH", "NARAYANA HRUDAYALAYA",
    "NTPC", "NYKAA", "FSN E-COMMERCE","OLAELEC", "OLA ELECTRIC", "POLYCAB", "PROTEAN",
    "RELIANCE", "RIL", "PROTEAN eGOV TECHNOLOGIES", "PROTEAN eGOV", "RELIANCE INDUSTRIES", "RELIANCE JIO", "RELIANCE RETAIL",
    "SAILIFE", "SAI LIFE", "SBIN", "SBI", "STATE BANK", "SEPC", "SHAKTIPUMP", "SHakTI PUMPS",
    "SHRIRAMFIN", "SHRIRAM FINANCE", "SJS", "SJS ENTERPRISES", "SKIPPER", "SONACOMS", "SONA BLW",
    "SUZLON", "SUZLON ENERGY", "TATASTEEL", "TATA STEEL", "TCS", "TIPSMUSIC", "TIPS MUSIC",
    "TITAN", "TITAN COMPANY", "TVSMOTOR", "TVS MOTOR", "URBANCO", "URBAN COMPANY",
    "WABAG", "VA TECH WABAG", "WAAREEENER", "WAAREE ENERGIES", "YATHARTH", "YATHARTH HOSPITAL",
    "YATRA", "YATRA ONLINE", "ZAGGLE", "ZAGGLE PREPAID", "physicswallah", "physics wallah", "pwl ", "physicswallah limited"
]

MARKET_KEYWORDS = [
    "rbi rate", "repo rate", "rate cut", "rate hike", "fed decision", "fomc", "interest rate", "rate",
    "warsh", "kevin warsh", "malhotra", "sanjay malhotra", "shaktikanta", "shaktikanta das", "monetary policy",
    "వడ్డీ రేటు", "రెపో రేటు",
    "budget 2026", "union budget", "budget", "gst rate change", "government policy", "corporate tax",
    "cabinet decision", "import duty", "export ban", "government decision", "govt decision", "gdp growth",
    "us gdp", "cpi inflation", "india gdp", "inflation", "gdp", "cabinet meeting",
    "ప్రభుత్వ నిర్ణయం", "బడ్జెట్", "ద్రవ్యోల్బణం",
    "war", "strike", "strikes", "attack", "attacks", "military", "sanctions", "iran", "us-iran",
    "crude", "oil", "brent", "opec", "omc", "dollar", "crude spike", "above $", "surge",
    "యుద్ధం", "దాడి", "దాడులు", "సైనిక", "ఆంక్షలు", "ఇరాన్", "చమురు", "క్రూడ్", "డాలర్", "crude oil",
    "market crash", "circuit breaker", "scam", "sebi ban", "emergency", "urgent", "breaking",
    "అత్యవసర", "rbi mpc", "mpc", "rupee", "రూపాయి"
]

IMPORTANT_KEYWORDS = MARKET_KEYWORDS + [stock.lower() for stock in MY_WATCHLIST]

news_feeds = [
    "https://www.forexlive.com/rss",
    "https://www.investing.com/rss/news_1.rss",
    "https://www.investing.com/rss/news_301.rss",
]

TIMINGS = {
    "GIFT Nifty": ("06:30", "02:45"),
    "Nikkei (Japan)": ("05:30", "11:30"),
    "Hang Seng (HK)": ("06:45", "13:30"),
    "DAX (Germany)": ("12:30", "21:00"),
    "FTSE (UK)": ("12:30", "21:00"),
    "Dow Jones (US)": ("19:00", "01:30"),
    "Nasdaq (US)": ("19:00", "01:30"),
    "S&P 500 (US)": ("19:00", "01:30"),
    "Gold (Commodity)": ("04:30", "03:30"),
    "Silver (Commodity)": ("04:30", "03:30"),
    "Brent Oil": ("05:30", "03:30"),
    "WTI Crude (US Oil)": ("03:30", "02:30"),
    "US 10Y Yield": ("18:30", "03:30"),
    "Bitcoin (Daily)": ("05:30", "05:29"),
}

symbols = {
    "GIFT Nifty": "^NSEI",
    "Dow Jones (US)": "^DJI",
    "Nasdaq (US)": "^IXIC",
    "S&P 500 (US)": "^GSPC",
    "Nikkei (Japan)": "^N225",
    "Hang Seng (HK)": "^HSI",
    "DAX (Germany)": "^GDAXI",
    "FTSE (UK)": "^FTSE",
    "Gold (Commodity)": "GC=F",
    "Silver (Commodity)": "SI=F",
    "Brent Oil": "BZ=F",
    "WTI Crude (US Oil)": "CL=F",
    "Bitcoin (Daily)": "BTC-USD",
    "US 10Y Yield": "^TNX",
}

# ==========================================================
# 🔍 LOGGING & MONGO UTILITIES (పాత మెమరీ లిస్ట్‌ల ప్లేస్‌లో మోంగో)
# ==========================================================
def log(msg, level="INFO"):
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] [{level}] {msg}")

def translate_to_telugu(text):
    try: return GoogleTranslator(source='auto', target='te').translate(text)
    except: return text

def translate(text): return translate_to_telugu(text)

def safe_html_text(text):
    if not text: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def safe_html_url(url):
    if not url: return ""
    return str(url).replace("&", "&amp;").replace('"', '&quot;').replace("'", "&#39;")

def clean_html_tags(text):
    if not text: return ""
    return re.sub('<[^>]+>', '', text).strip()

def check_if_important(text_to_check):
    if not text_to_check: return False
    lowercase_text = text_to_check.lower()
    for keyword in IMPORTANT_KEYWORDS:
        if re.search(r'\b' + re.escape(keyword) + r'\b', lowercase_text): return True
    return False

def is_duplicate_news(new_title):
    if not new_title: return False
    def clean_for_compare(t): return set(re.findall(r'\w+', t.lower()))
    new_words = clean_for_compare(new_title)
    if not new_words: return False
    now = datetime.now(IST)
    cutoff = now - timedelta(minutes=15)
    for n in reversed(rss_news_store):
        if isinstance(n, dict) and n.get('time') >= cutoff:
            existing_words = clean_for_compare(n.get('title', ''))
            if not existing_words: continue
            intersection = new_words.intersection(existing_words)
            smaller_len = min(len(new_words), len(existing_words))
            if smaller_len > 0:
                match_percentage = (len(intersection) / smaller_len) * 100
                if match_percentage >= 80: return True
    return False

# 🍃 MongoDB క్లీనప్ మరియు చెకింగ్ ఫంక్షన్స్
def is_link_sent(link):
    return db_links.find_one({"link": link}) is not None

def add_sent_link(link):
    db_links.insert_one({"link": link, "timestamp": datetime.now(IST)})
    if db_links.count_documents({}) > 500:
        oldest = db_links.find().sort([("timestamp", 1)]).limit(100)
        for doc in oldest: db_links.delete_one({"_id": doc["_id"]})

def is_title_sent(title):
    return db_news.find_one({"title": title}) is not None

def add_sent_title(title):
    db_news.insert_one({"title": title, "timestamp": datetime.now(IST)})
    if db_news.count_documents({}) > 500:
        oldest = db_news.find().sort([("timestamp", 1)]).limit(100)
        for doc in oldest: db_news.delete_one({"_id": doc["_id"]})

def is_live_result_sent(event_id):
    return db_live_results.find_one({"event_id": event_id}) is not None

def add_live_result_sent(event_id):
    db_live_results.insert_one({"event_id": event_id, "timestamp": datetime.now(IST)})

# ==========================================================
# 💬 TELEGRAM MESSAGE SENDING HANDLERS
# ==========================================================
def send_long_message(chat_id, text, parse_mode='HTML'):
    if len(text) <= 3800:
        try: bot.send_message(chat_id, text, parse_mode=parse_mode, disable_web_page_preview=True)
        except Exception as e:
            log(f"⚠ HTML Parse Error, sending plain text: {e}", "WARNING")
            bot.send_message(chat_id, clean_html_tags(text))
        return

    lines = text.split('\n\n')
    current_chunk = ""
    for line in lines:
        if len(current_chunk) + len(line) + 2 > 3800:
            try: bot.send_message(chat_id, current_chunk, parse_mode=parse_mode, disable_web_page_preview=True)
            except: bot.send_message(chat_id, clean_html_tags(current_chunk))
            current_chunk = line + '\n\n'
        else: current_chunk += line + '\n\n'
    if current_chunk:
        try: bot.send_message(chat_id, current_chunk, parse_mode=parse_mode, disable_web_page_preview=True)
        except: bot.send_message(chat_id, clean_html_tags(current_chunk))

def safe_send(msg, chat_id=CHAT_ID, parse_mode="HTML", disable_preview=True):
    MAX_LENGTH = 4000
    parts = [msg[i:i+MAX_LENGTH] for i in range(0, len(msg), MAX_LENGTH)] if len(msg) > MAX_LENGTH else [msg]
    for part in parts:
        for i in range(3):
            try:
                bot.send_message(chat_id, part, parse_mode=parse_mode, disable_web_page_preview=True)
                break
            except Exception as e:
                print(f"Retry {i+1}: {e}")
                time.sleep(3)

# 🎙️ వాయిస్ నోట్స్ అలర్ట్స్ పూర్తిగా తీసేశాము సర్, ర్యామ్ అస్సలు పెరగదు
def send_vip_voice_alert(text, image_url=None, source_type="NORMAL"):
    if not VIP_CHAT_ID: return
    try:
        cleaned_msg_body = text
        bad_headers = ["🚨🚨 <b>IMPORTANT MARKET ALERT</b> 🚨🚨\n\n", "🚀 <b>IMPORTANT X UPDATE</b> 🚨\n\n", "👑 <b>[VIP PREMIUM ALERT]</b> 👑\n\n", "⚡ <b>🎯 30-MINUTES MARKET PULSE</b>", "🔔 <b>లైవ్ రిజల్ట్ అప్డేట్!</b>\n\n"]
        for header in bad_headers: cleaned_msg_body = cleaned_msg_body.replace(header, "")
        if cleaned_msg_body.startswith("📌 "): cleaned_msg_body = cleaned_msg_body.replace("📌 ", "", 1)

        if source_type == "NORMAL": vip_header = "<b>🌍 NRSS</b>\n\n"
        elif source_type == "X": vip_header = "<b>🐦 XRSS</b>\n\n"
        elif "ECONOMIC" in source_type: vip_header = f"👑 <b>{source_type}</b>\n\n"
        else: vip_header = f"<b>👑 {source_type}</b>\n\n"

        final_vip_msg = f"{vip_header}{cleaned_msg_body}"

        if image_url and str(image_url).startswith('http'):
            try: bot.send_photo(VIP_CHAT_ID, image_url, caption=final_vip_msg[:1024], parse_mode='HTML')
            except Exception:
                try: bot.send_photo(VIP_CHAT_ID, image_url, caption=clean_html_tags(final_vip_msg)[:1024])
                except:
                    preview_msg = f'<a href="{image_url}"></a>{final_vip_msg}'
                    try: bot.send_message(VIP_CHAT_ID, preview_msg, parse_mode='HTML', disable_web_page_preview=False)
                    except: bot.send_message(VIP_CHAT_ID, clean_html_tags(preview_msg), disable_web_page_preview=False)
        else:
            try: send_long_message(VIP_CHAT_ID, final_vip_msg, parse_mode='HTML')
            except: send_long_message(VIP_CHAT_ID, clean_html_tags(final_vip_msg))

        log(f"🚀 VIP {source_type} Alert Sent successfully.")
    except Exception as e: log(f"❌ VIP Alert Process Error: {e}", "ERROR")

def get_image_url(entry):
    try:
        if hasattr(entry, 'media_content') and entry.media_content:
            url = entry.media_content[0]['url']
            if str(url).startswith('http'): return url
        summary_raw = entry.get('summary') or entry.get('description') or ""
        soup = BeautifulSoup(str(summary_raw), 'html.parser')
        img = soup.find('img')
        if img and img.get('src'):
            url = img['src']
            if str(url).startswith('http'): return url
    except: return None
    return None

def manage_memory():
    global rss_news_store
    if len(rss_news_store) > MAX_NEWS:
        rss_news_store = rss_news_store[CLEAR_COUNT:]
        log(f"✅ Memory cleaned.")

def auto_unpin_old_messages():
    global pinned_messages_store
    now = datetime.now(IST)
    cutoff_time = now - timedelta(days=2)
    remaining_pins = []
    for item in pinned_messages_store:
        if item["time"] < cutoff_time:
            try: bot.unpin_chat_message(CHAT_ID, item["message_id"])
            except Exception as e: log(f"⚠ Unpin Error: {e}", "WARNING")
        else: remaining_pins.append(item)
    pinned_messages_store = remaining_pins

def clean_x_text(text):
    junk = [r'http\S+', r'www\.\S+', r'@\w+', r'#\w+', r'环境', r'\|']
    for p in junk: text = re.sub(p, '', text, flags=re.IGNORECASE)
    return clean_html_tags(re.sub(r'\s+', ' ', text).strip())

# ==========================================================
# 🤖 AI ENGINE CLIENTS & METHODS
# ==========================================================
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

def safe_gemini(prompt):
    if not client: return "AI Key Error"
    for i in range(3):
        try:
            response = client.models.generate_content(model=MODEL_NAME, contents=prompt)
            return response.text
        except Exception as e:
            print(f"Gemini Retry {i+1}: {e}")
            time.sleep(5)
    return "AI అందుబాటులో లేదు"

def ask_gemini_raw(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200: return response.json()['candidates'][0]['content']['parts'][0]['text']
    except Exception as e: log(f"Gemini API Exception: {e}", "ERROR")
    return None

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

def ask_groq_raw(prompt):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200: return response.json()['choices'][0]['message']['content']
    except Exception as e: print(f"Groq Exception: {str(e)}")
    return None

def get_groq_analysis(prompt_text):
    if not groq_client: return "Groq Client Not Init"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            chat_completion = groq_client.chat.completions.create(
                messages=[{"role": "user", "content": f"మీరు ఒక stock market నిపుణుడు. ఈ డేటాను చదివి, చంటి గారికి అర్థమయ్యేలా 2-3 సులభమైన తెలుగు వాక్యాల్లో విశ్లేషణ ఇవ్వండి. మార్కెట్ పెరుగుతుందా లేదా తగ్గుతుందా అని చెప్పండి: {prompt_text}"}],
                model="llama-3.3-70b-versatile",
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            log(f"Groq AI Error (Attempt {attempt+1}): {e}")
            time.sleep(5)
    return "AI విశ్లేషణ ప్రస్తుతం అందుబాటులో లేదు."

def get_vip_event_better_summary(event_name, country, actual, estimate, previous):
    if not groq_client: return "ఆర్థిక ఈవెంట్ విశ్లేషణ ప్రస్తుతం అందుబాటులో లేదు."
    prompt = f"""
    You are an expert global macro hedge fund manager and analyst.
    Analyze this live economic calendar event and write a highly refined, professional summary in Telugu.
    Explain what this data means for the global or regional stock market, and whether it is positive, negative, or neutral for market volatility.

    Data Details:
    Event: {event_name}
    Country: {country}
    Actual Value: {actual}
    Expected Estimate: {estimate}
    Previous Value: {previous}

    Provide a robust 4-line breakdown in clean Telugu. No English text, no markdown.
    """
    try:
        completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile"
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"ఈవెంట్ విశ్లేషణ లోడింగ్ లోపం: {str(e)[:50]}"

# ==========================================================
# 📈 MARKET DATA ENGINE & GAP ALERTS
# ==========================================================
def is_market_open(name):
    now_ist = datetime.now(IST)
    if "Bitcoin" in name or "BTC" in name: return "🟢"
    if any(x in name for x in ["GIFT Nifty", "WTI Crude", "Brent", "Gold", "Silver"]): return "🟢"

    mapping = {
        "Nikkei": (JP, "09:00", "15:00"), "Hang Seng": (HK, "09:30", "16:00"),
        "DAX": (EU, "09:00", "17:30"), "FTSE": (EU, "08:00", "16:30"),
        "Dow": (US, "09:30", "16:00"), "Nasdaq": (US, "09:30", "16:00"),
        "S&P": (US, "09:30", "16:00"), "10Y": (US, "08:00", "17:00")
    }
    for key, (tz, start, end) in mapping.items():
        if key in name:
            now_local = now_ist.astimezone(tz).time()
            if datetime.strptime(start, "%H:%M").time() <= now_local < datetime.strptime(end, "%H:%M").time(): return "🟢"
            return "🔴"
    return "🔴"

def get_data(symbol):
    try:
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}", headers=HEADERS, timeout=10)
        result = r.json()["chart"]["result"][0]
        meta = result["meta"]
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        if (price is None or price == 0) and "indicators" in result:
            closes = [c for c in result["indicators"]["quote"][0].get("close", []) if c]
            if closes: price = closes[-1]
        prev_close = meta.get("previousClose") or meta.get("chartPreviousClose")
        return price, prev_close
    except: return None, None

def check_gap_alert(name, price, prev_close, current_date):
    if not price or not prev_close: return
    gap_percent = ((price - prev_close) / prev_close) * 100
    gap_key = f"{name}_{current_date}_gap"
    
    if gap_key not in gap_alert_sent and abs(gap_percent) >= 1.0:
        direction = "📈 <b>GAP UP</b>" if gap_percent > 0 else "📉 <b>GAP DOWN</b>"
        
        msg = (
            f"🚨 <b>GAP ALERT!</b>\n\n"
            f"📊 <b>స్టాక్/ఇండెక్స్:</b> {html.escape(str(name))}\n"
            f"{direction}: <b>{gap_percent:+.2f}%</b>\n"
            f"🔹 <b>Current Price:</b> {price:.2f}\n"
            f"🔸 <b>Prev Close:</b> {prev_close:.2f}"
        )
        
        safe_send(msg)
        gap_alert_sent[gap_key] = True

# ==========================================================
# 📅 FREE ECONOMIC CALENDAR INTEGRATION (FREE ECOCAL)
# ==========================================================
def fetch_economic_calendar_data(start_date, end_date):
    global economic_calendar_cache
    cache_key = f"{start_date}_{end_date}"

    if cache_key in economic_calendar_cache:
        log("📅 Economic calendar data from cache")
        return economic_calendar_cache[cache_key]

    try:
        log(f"📅 Fetching economic calendar from {start_date} to {end_date}...")
        ec = Calendar(startHorizon=start_date, endHorizon=end_date, withDetails=True, nbThreads=1)
        df = ec.getCalendar()

        if df is None or df.empty:
            log("⚠ Economic calendar empty")
            return None

        economic_calendar_cache[cache_key] = df
        log(f"✅ Economic calendar loaded: {len(df)} events")
        return df
    except Exception as e:
        log(f"❌ Economic calendar fetch error: {e}", "ERROR")
        return None

def fetch_economic_calendar(days=1):
    try:
        now_ist = datetime.now(IST)
        start_datetime = now_ist
        end_datetime = now_ist + timedelta(hours=24)
        
        start_date = start_datetime.strftime('%Y-%m-%d')
        end_date = end_datetime.strftime('%Y-%m-%d')
        
        log(f"📅 Fetching 24hr calendar from {start_datetime.strftime('%Y-%m-%d %H:%M')} to {end_datetime.strftime('%Y-%m-%d %H:%M')}")
        df = fetch_economic_calendar_data(start_date, end_date)
        
        if df is None or df.empty:
            return ["☀ <b>తదుపరి 24 గంటల్లో ఎటువంటి ఈవెంట్స్ లేవు చంటి గారు.</b>"]
        
        df['DateTime'] = pd.to_datetime(df['Start'], format='%m/%d/%Y %H:%M:%S', errors='coerce', utc=True).dt.tz_convert(IST)
        df = df[(df['DateTime'] >= start_datetime) & (df['DateTime'] <= end_datetime)]
        
        if df.empty:
            return ["☀ <b>తదుపరి 24 గంటల్లో ఈవెంట్స్ ఏవీ లేవు చంటి గారు.</b>"]
            
        allowed_countries = ['IN', 'IND', 'US', 'USA', 'CN', 'CHN', 'JP', 'JPN', 'EU', 'EUR']
        df = df[df['countryCode'].notna() & df['countryCode'].str.upper().str.strip().isin(allowed_countries)]
        
        if df.empty:
            return ["☀ <b>ఎంచుకున్న దేశాల (IN, US, CN, JP, EU) ఈవెంట్స్ ఏవీ లేవు చంటి గారు.</b>"]
        
        impact_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        df['ImpactOrder'] = df['Impact'].str.upper().map(impact_order)
        df = df.sort_values(by=['ImpactOrder', 'DateTime'])
        
        messages_list = []
        report = f"📅 <b>తదుపరి 24 గంటల ఆర్థిక క్యాలెండర్</b>\n🕒 {start_datetime.strftime('%d-%b %I:%M %p')} నుండి {end_datetime.strftime('%d-%b %I:%M %p')} వరకు\n\n"
        
        country_names = {'US': 'United States 🇺🇸', 'USA': 'United States 🇺🇸', 'IN': 'India 🇮🇳', 'IND': 'India 🇮🇳', 'JP': 'Japan 🇯🇵', 'JPN': 'Japan 🇯🇵', 'CN': 'China 🇨🇳', 'CHN': 'China 🇨🇳', 'EU': 'Euro Zone 🇪🇺', 'EUR': 'Euro Zone 🇪🇺'}
        
        for _, row in df.iterrows():
            impact_val = str(row['Impact']).upper()
            impact_icon = "🔴 High" if impact_val == 'HIGH' else ("🟡 Medium" if impact_val == 'MEDIUM' else "⚪ Low")
            event_time = row['DateTime'].strftime('%d %b, %I:%M %p')
            
            c_code = str(row['countryCode']).upper().strip()
            country = html.escape(str(country_names.get(c_code, c_code)))
            event_name = html.escape(str(row['Name']))
            telugu_name = html.escape(str(translate_to_telugu(row['Name'])))
            
            actual_val = html.escape(str(row['actual'])) if pd.notna(row['actual']) else "Waiting... ⏳"
            forecast_val = html.escape(str(row['consensus'])) if pd.notna(row['consensus']) else "N/A"
            prev_val = html.escape(str(row['previous'])) if pd.notna(row['previous']) else "N/A"
            
            event_block = (f"📅 <b>{event_time}</b>\n🌍 {country} | {event_name}\n📝 <b>వివరణ:</b> {telugu_name}\n"
                           f"✅ Actual: <b>{actual_val}</b> | Est: {forecast_val} | Prev: {prev_val}\n🔥 ఇంపాక్ట్: {impact_icon}\n──────────────────\n\n")
            
            if len(report) + len(event_block) > 3500:
                messages_list.append(report)
                report = f"📅 <b>తదుపరి 24 గంటల ఆర్థిక క్యాలెండర్ (Contd...)</b>\n\n" + event_block
            else:
                report += event_block
        
        if report: messages_list.append(report)
        return messages_list 
    except Exception as e:
        log(f"❌ Calendar report error: {e}", "ERROR")
        return [f"❌ సమస్య వచ్చింది: {html.escape(str(e)[:150])}"]
    
def check_for_live_updates():
    try:
        today = datetime.now(IST).strftime('%Y-%m-%d')
        df = fetch_economic_calendar_data(today, today)
        if df is None or df.empty: return

        allowed_countries = ['IN', 'IND', 'US', 'USA', 'CN', 'CHN', 'JP', 'JPN', 'EU', 'EUR']
        country_names = {'US': 'United States 🇺🇸', 'USA': 'United States 🇺🇸', 'IN': 'India 🇮🇳', 'IND': 'India 🇮🇳', 'JP': 'Japan 🇯🇵', 'JPN': 'Japan 🇯🇵', 'CN': 'China 🇨🇳', 'CHN': 'China 🇨🇳', 'EU': 'Euro Zone 🇪🇺', 'EUR': 'Euro Zone 🇪🇺'}

        for _, row in df.iterrows():
            if str(row['Impact']).upper() not in ['HIGH', 'MEDIUM']: continue
            c_code = str(row['countryCode']).upper().strip() if pd.notna(row['countryCode']) else ""
            if c_code not in allowed_countries: continue

            event_name = row['Name']
            country = country_names.get(c_code, c_code)
            actual = str(row['actual']).strip() if pd.notna(row['actual']) else ""

            if not actual or actual in ['nan', 'None', 'Waiting... ⏳']: continue
            event_id = f"{event_name}_{c_code}_{today}"

            # 🎯 🎯 FIX: ఇక్కడ లోకల్ సెట్ కి బదులు పక్కాగా MongoDB ద్వారా డూప్లికేట్ చెక్ పెట్టాము సర్
            if is_live_result_sent(event_id): continue

            estimate = str(row['consensus']) if pd.notna(row['consensus']) else "N/A"
            prev = str(row['previous']) if pd.notna(row['previous']) else "N/A"
            display_time = row['DateTime'].strftime('%I:%M %p') if 'DateTime' in row else "Today"

            ai_analysis = get_groq_analysis(f"Event: {event_name}, Actual: {actual}, Expected: {estimate}, Country: {country}")
            telugu_event_name = translate_to_telugu(event_name)

            msg = (f"🔔 <b>లైవ్ రిజల్ట్ అప్డేట్! ({country})</b>\n──────────────────────\n"
                   f"📊 <b>ఈవెంట్:</b> {html.escape(str(event_name))}\n"
                   f"📝 <b>వివరణ:</b> {html.escape(str(telugu_event_name))}\n🕒 <b>సమయం:</b> {display_time}\n\n"
                   f"✅ <b>Actual:</b> <code>{html.escape(str(actual))}</code>\n📉 <b>Expected:</b> {html.escape(str(estimate))}\n"
                   f"🔄 <b>Previous:</b> {html.escape(str(prev))}\n──────────────────────\n🤖 <b>AI విశ్లేషణ:</b>\n{ai_analysis}")
            
            safe_send(msg, chat_id=CHAT_ID)

            if VIP_CHAT_ID:
                better_summary = get_vip_event_better_summary(event_name, country, actual, estimate, prev)
                vip_msg = (f"🌍 <b>ఈవెంట్:</b> {html.escape(str(event_name))} ({country})\n📝 <b>వివరణ:</b> {html.escape(str(telugu_event_name))}\n"
                           f"🕒 <b>సమయం:</b> {display_time}\n\n✅ <b>Actual:</b> <code>{html.escape(str(actual))}</code> | Est: {estimate} | Prev: {prev}\n"
                           f"──────────────────────\n📊 <b>VIP ఎకనామిక్ డీప్ సమ్మరీ:</b>\n{better_summary}")
                send_vip_voice_alert(vip_msg, image_url=None, source_type="📊 ECONOMIC UPDATE")

            # 🎯 🎯 FIX: పంపించిన లైవ్ రిజల్ట్స్ ని వెంటనే MongoDB లో స్టోర్ చేస్తున్నాం
            add_live_result_sent(event_id)
            time.sleep(1) 
    except Exception as e:
        log(f"❌ Live Economic Update Error: {e}", "ERROR")

# ==========================================================
# 🔄 LIVE RSS LOOPS & FEEDS
# ==========================================================
def fetch_normal_rss():
    log("🌍 NORMAL RSS STARTED...")
    while True:
        for name, url in RSS_FEEDS.items():
            try:
                res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
                feed = feedparser.parse(res.content)
                if not feed.entries: continue

                for entry in feed.entries[:10]:
                    link = entry.get("link", "").strip()
                    title = clean_html_tags(entry.get("title", ""))
                    tel_title = translate(title)

                    # 🎯 🎯 FIX: పాత `sent_links` కి బదులు ఇక్కడ MongoDB చెక్ పెట్టాము సర్!
                    if not link or is_link_sent(link) or is_duplicate_news(tel_title) or is_duplicate_news(title): continue
                    
                    add_sent_link(link)
                    
                    summary_raw = entry.get("summary") or entry.get("description") or ""
                    clean_desc = clean_html_tags(summary_raw).replace("\n", " ")
                    tel_desc = translate(clean_desc[:800])
                    
                    msg = (f"📌 <b>{safe_html_text(tel_title)}</b>\n\n🇬🇧 <b>English Title:</b>\n{safe_html_text(title)}\n\n"
                           f"🇮🇳 <b>తెలుగు సమ్మరీ:</b>\n{safe_html_text(tel_desc)}\n\n"
                           f"🌐 <b>{safe_html_text(name)}</b>\n"
                           f'🔗 <a href="https://translate.google.com/translate?sl=en&tl=te&u={link}">Read More in Telugu</a> | <a href="{link}">English Original</a>')
                    
                    ist_now = datetime.now(IST)
                    rss_news_store.append({"time": ist_now, "type": "NORMAL", "source": name, "title": tel_title, "desc": tel_desc, "link": link, "full_text": title + " " + clean_desc})
                    manage_memory()

                    try: bot.send_message(CHAT_ID, msg, parse_mode='HTML', disable_web_page_preview=False)
                    except Exception as e: log(f"❌ Telegram error: {e}", "ERROR")
                    time.sleep(1)
            except Exception as e: log(f"❌ RSS Error {name}: {e}", "ERROR")
        time.sleep(120)

def fetch_x_rss():
    log("🐦 X RSS STARTED...")
    scraper = cloudscraper.create_scraper()
    while True:
        for name, url in X_RSS_FEEDS.items():
            try:
                res = scraper.get(url, timeout=20)
                if res.status_code != 200: continue
                feed = feedparser.parse(res.content)

                for entry in feed.entries[:5]:
                    link = entry.get("link", "").strip()
                    title = clean_x_text(entry.get("title", ""))
                    tel_title = translate(title)

                    # 🎯 🎯 FIX: ఇక్కడ కూడా గిట్‌హబ్ కోడ్‌లోని పాత వేరియబుల్ తీసేసి MongoDB చెక్ పెట్టాము
                    if not link or is_link_sent(link) or is_duplicate_news(tel_title) or is_duplicate_news(title): continue
                    add_sent_link(link)
                    
                    is_important = check_if_important(title) or check_if_important(tel_title)
                    g_trans_url = f"https://translate.google.com/translate?sl=en&tl=te&u={link}"

                    header = f"🚀 <b>{safe_html_text(name)} Update</b>\n\n"
                    msg = f"{header}📌 <b>{safe_html_text(tel_title)}</b>\n\n🇬🇧 {safe_html_text(title)}\n\n🔗 <a href='{g_trans_url}'>Read More in Telugu</a> | <a href='{link}'>English Original</a>"
                    
                    ist_now = datetime.now(IST)
                    rss_news_store.append({"time": ist_now, "type": "X", "source": name, "title": tel_title, "link": link})
                    manage_memory()

                    image_url = get_image_url(entry)
                    try:
                        if image_url:
                            try: sent_msg = bot.send_photo(CHAT_ID, image_url, caption=msg[:1024], parse_mode='HTML')
                            except Exception: sent_msg = bot.send_photo(CHAT_ID, image_url, caption=clean_html_tags(msg)[:1024])
                        else:
                            sent_msg = bot.send_message(CHAT_ID, msg, parse_mode='HTML', disable_web_page_preview=False)
                        
                        if is_important and VIP_CHAT_ID and name != "Redbox X":
                            vip_msg = f"📌 <b>{safe_html_text(tel_title)}</b>\n\n🇬🇧 {safe_html_text(title)}\n\n🔗 <a href='{g_trans_url}'>Read More in Telugu</a> | <a href='{link}'>English Original</a>"
                            send_vip_voice_alert(vip_msg, image_url=image_url, source_type="X")
                            bot.pin_chat_message(CHAT_ID, sent_msg.message_id, disable_notification=False)
                            pinned_messages_store.append({"message_id": sent_msg.message_id, "time": ist_now})
                            auto_unpin_old_messages()
                    except Exception as e: log(f"❌ X Telegram Error: {e}", "ERROR")
                    time.sleep(2)
            except Exception as e: log(f"❌ X RSS Error {name}: {e}", "ERROR")
        time.sleep(120)

# ==========================================================
# ⏱️ 30 MINUTE MARKET PULSE THREAD
# ==========================================================
def half_hourly_market_pulse_loop():
    log("⏱️ 30 MINUTE AI SMART PULSE THREAD STARTED...")
    while True:
        now = datetime.now(IST)
        minutes_to_add = 30 - (now.minute % 30)
        next_run = (now + timedelta(minutes=minutes_to_add)).replace(second=0, microsecond=0)
        time.sleep((next_run - now).total_seconds())
        
        if not (6 <= next_run.hour <= 22) or (next_run.hour == 22 and next_run.minute > 0): continue
        time_str = next_run.strftime('%I:%M %p')
        night_notice = "\n\n🌙 <b>చంటి గారు, ఈరోజుకు ఆటోమేటిక్ పల్స్ రిపోర్ట్స్ పూర్తయ్యాయి. మళ్లీ రేపు ఉదయం 6:00 AM ki వస్తుంది సార్.</b>" if next_run.hour == 22 and next_run.minute == 0 else ""

        try:
            if next_run.hour == 6 and next_run.minute == 0:
                cutoff_time = next_run - timedelta(hours=10)
                intro_text = "(రాత్రి 08:00 PM నుండి ఉదయం 6:00 AM వరకు వచ్చిన కీలకమైన వార్తలు)"
            else:
                cutoff_time = next_run - timedelta(minutes=30)
                intro_text = "(గత 30 నిమిషాల అత్యంత కీలకమైన వార్తలు)"
            
            recent_news_for_ai = []
            news_lookup_dict = {} 
            for n in rss_news_store:
                if isinstance(n, dict) and n.get('time') >= cutoff_time and n.get('type') == "NORMAL":
                    eng_title = n.get('full_text', '').split("   ")[0]
                    recent_news_for_ai.append(f"ID: {len(recent_news_for_ai)} | Title: {eng_title}")
                    news_lookup_dict[str(len(recent_news_for_ai)-1)] = n

            if not recent_news_for_ai:
                no_news_msg = f"⚡ <b>🎯 30-MINUTES MARKET PULSE ({time_str})</b> ⚡\n📌 <b>మార్కెట్ అప్‌డేట్:</b> ఈ అరగంటలో కీలకమైన వార్తలు ఏవీ రాలేదు సార్.\n\n{night_notice}"
                bot.send_message(CHAT_ID, no_news_msg, parse_mode='HTML')
                if VIP_CHAT_ID: send_vip_voice_alert(no_news_msg, source_type="PULSE")
                continue

            pulse_filter_prompt = f"""
            You are an elite stock market filter bot. Review these news titles.
            Strictly REMOVE/IGNORE any cinema, movies, entertainment, celebrity gossip, or pop culture news.
            Keep ONLY high-quality corporate events, major company business updates, corporate deals, mergers, policy changes, national economy, and international financial macro news.
            Return ONLY a comma-separated list of the ID numbers that pass this filter.
            """
            ai_response = ask_groq_raw(pulse_filter_prompt + "\n".join(recent_news_for_ai))
            recent_important_news = []
            
            if ai_response and ai_response.strip():
                important_ids = re.findall(r'\d+', ai_response)
                for news_id in important_ids:
                    if news_id in news_lookup_dict:
                        n = news_lookup_dict[news_id]
                        raw_title = n.get('full_text', '').split("   ")[0]
                        
                        subject_match = re.search(r'\b[a-zA-Z0-9\s\&]+', raw_title)
                        if subject_match:
                            full_subject = subject_match.group(0).strip()
                            words = full_subject.split()
                            subject_title = " ".join(words[:3]) if len(words) > 3 else full_subject
                        else: subject_title = "Market Update"
                        
                        news_block = (f"<b>{subject_title}:-</b>\n  {safe_html_text(n.get('title', ''))}\n"
                                      f"<b>సమ్మరీ:-</b>\n  {safe_html_text(n.get('desc', ''))}")
                        recent_important_news.append(news_block)

            if not recent_important_news:
                no_news_msg = f"⚡ <b>🎯 30-MINUTES MARKET PULSE ({time_str})</b> ⚡\n📌 <b>మార్కెట్ అప్‌డేట్:</b> ఆర్థిక లేదా కార్పొరేట్ వార్తలు ఏవీ రాలేదు సార్.\n\n{night_notice}"
                bot.send_message(CHAT_ID, no_news_msg, parse_mode='HTML')
                if VIP_CHAT_ID: send_vip_voice_alert(no_news_msg, source_type="PULSE")
                continue

            recent_important_news = list(dict.fromkeys(recent_important_news))
            pulse_body = "\n\n🔹🔹🔹\n\n".join(recent_important_news)
            full_report_msg = f"⚡ <b>🎯 30-MINUTES MARKET PULSE ({time_str})</b> ⚡\n{intro_text}\n\n{pulse_body}{night_notice}"
            
            send_long_message(CHAT_ID, full_report_msg, parse_mode='HTML')
            if VIP_CHAT_ID: send_vip_voice_alert(full_report_msg, source_type="PULSE")
        except Exception as e: log(f"❌ Pulse Error: {e}", "ERROR")

def send_market_table():
    table_content = f"{'-' * 52}\n{'Mkt':<14} {'Price':>9} {'+/-Pts':>8} {'%':>6} {'Trnd':>4}\n{'-' * 52}\n"
    current_date = datetime.now(IST).date()
    for name, sym in symbols.items():
        price, prev_close = get_data(sym)
        if price and prev_close:
            diff = price - prev_close
            change = (diff / prev_close) * 100
            check_gap_alert(name, price, prev_close, current_date)
            trend = "📈" if change > 0.3 else ("📉" if change < -0.3 else "➖")
            status = is_market_open(name)
            short_name = name.split(' (')[0][:11]
            table_content += f"{status}{short_name:<12} {price:>9.1f} {diff:>8.1f} {change:>5.1f}% {trend:>2}\n"
    try: safe_send(f"📊 <b>Global Market Live</b>\n<pre>{table_content}</pre>")
    except Exception as e: print(e)

# ==========================================================
# ⚙️ MAIN LOOP & WORKERS
# ==========================================================
def ai_worker():
    while True:
        text, chat_id = ai_queue.get()
        try:
            res_text = safe_gemini(f"Explain this news in 6 lines Telugu: {text}")
            safe_send(f"🧠 <b>AI విశ్లేషణ:</b>\n{res_text}", chat_id=chat_id)
        except: pass
        ai_queue.task_done()
        time.sleep(10)

def main_loop():
    global last_reset_date, economic_calendar_cache
    while True:
        try:
            now_ist = datetime.now(IST)
            now_ist_str = now_ist.strftime("%H:%M")
            current_date = now_ist.date()
            
            if current_date > last_reset_date:
                sent_alerts.clear()
                sudden_move_sent.clear()
                gap_alert_sent.clear()
                collected_news.clear()
                last_reset_date = current_date
                log("🔄 daily alerts reset successfully.")
                
                today_str = current_date.strftime('%Y-%m-%d')
                tomorrow_str = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
                valid_cache_keys = [f"{today_str}_{today_str}", f"{today_str}_{tomorrow_str}"]
                for key in list(economic_calendar_cache.keys()):
                    if key not in valid_cache_keys: del economic_calendar_cache[key]

            for m_name, (o_time, _) in TIMINGS.items():
                alert_id = f"{m_name}_{current_date}"
                if now_ist_str == o_time and alert_id not in sent_alerts:
                    safe_send(f"🔔 <b>MARKET OPEN ALERT</b>\n\n🚀 {m_name} ప్రారంభమైంది! (IST: {o_time})")
                    sent_alerts[alert_id] = True 

            for name, sym in symbols.items():
                if is_market_open(name) == "🟢":
                    price, prev_close = get_data(sym)
                    if price and prev_close:
                        change = ((price - prev_close) / prev_close) * 100
                        if abs(change) >= 1.50 and f"{name}_{current_date}_mv" not in sudden_move_sent:
                            safe_send(f"🚨 <b>VOLATILITY ALERT!</b>\n{name}: {change:.2f}% భారీ మార్పు!")
                            sudden_move_sent[f"{name}_{current_date}_mv"] = True 

            for f_url in news_feeds:
                feed = feedparser.parse(f_url)
                for e in feed.entries[:3]:
                    # 🎯 🎯 FIX: ఇక్కడ కూడా మెమరీ లిస్ట్‌కి బదులు పక్కాగా MongoDB చెక్ పెట్టాము సర్
                    if not is_title_sent(e.title):
                        add_sent_title(e.title)
                        collected_news.append(e.title) 
                        if len(collected_news) > 30: collected_news.pop(0) 
                        translated = translate_to_telugu(e.title)
                        safe_send(f"🌍 <b>{translated}</b>\n\n🌐 {e.title}\n🔗 <a href='{e.link}'>పూర్తి వార్త</a>", disable_preview=True)
                        if any(k in e.title.lower() for k in ["fed", "war", "oil", "inflation", "cpi", "rate cut"]):
                            ai_queue.put((e.title, CHAT_ID)) 
        except Exception as e: print(f"Error in global loop: {e}")
        gc.collect() 
        time.sleep(60)

def calculate_historical_target_time(hour_input):
    now = datetime.now(IST)
    target = now.replace(hour=hour_input, minute=0, second=0, microsecond=0)
    if hour_input >= now.hour: target = target - timedelta(days=1)
    return target

# ==========================================================
# 🤖 TELEGRAM BOT COMMAND HANDLERS
# ==========================================================
@bot.message_handler(commands=['start'])
def cmd_start(message): safe_send("🚀 <b>బాట్ రెడీ చంటి గారు! అన్ని ఫిల్టర్స్ లోడ్ అయ్యాయి.</b>", chat_id=message.chat.id)

@bot.message_handler(commands=['ram'])
def check_ram_usage_command(message):
    try:
        rss_count = len(rss_news_store)
        cal_keys_count = len(economic_calendar_cache)
        links_count = db_links.count_documents({})
        news_count = db_news.count_documents({})
        collected_count = len(collected_news)
        
        process = psutil.Process(os.getpid())
        total_ram_mb = process.memory_info().rss / (1024 * 1024)
        
        msg = (f"🖥️ <b>బాట్ లైవ్ ర్యామ్ నివేదిక (RAM Report)</b>\n──────────────────────\n"
               f"🌍 <b>RSS News Store:</b> {rss_count} వార్తలు\n"
               f"📅 <b>Calendar Cache:</b> {cal_keys_count} రోజులు\n\n"
               f"🍃 <b>MongoDB - Links Count:</b> {links_count} / 500\n"
               f"🍃 <b>MongoDB - News Count:</b> {news_count} / 500\n"
               f"📊 <b>Collected News Store:</b> {collected_count} వార్తలు\n──────────────────────\n"
               f"🚀 <b>TOTAL BOT RAM USAGE:</b>\n  🔥 <code>{total_ram_mb:.2f} MB</code>\n\n"
               f"📌 <i>ఆడియో క్లియర్ చేయడం వల్ల మరియు క్లౌడ్ డేТАబేస్ వల్ల ర్యామ్ సూపర్ సేఫ్‌గా ఉంది చంటి గారు!</i>")
        bot.reply_to(message, msg, parse_mode='HTML')
    except Exception as e: bot.reply_to(message, f"❌ ఎర్రర్: {str(e)}")

@bot.message_handler(commands=['summary'])
def summary(message):
    normal_news = [n['full_text'] for n in rss_news_store if isinstance(n, dict) and n.get('type') == "NORMAL"]
    if not normal_news: return bot.reply_to(message, "⚠️ వార్తలు లేవు సార్.")
    args = message.text.split()
    page = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
    per_page = 50
    total_pages = (len(normal_news) + per_page - 1) // per_page
    if page > total_pages: return
    sliced_news = list(reversed(normal_news))[(page - 1) * per_page : page * per_page]
    response_text = ask_gemini_raw("Analyze each news separately and organize into Section in Telugu.\n" + "\n".join(sliced_news))
    if response_text: send_long_message(CHAT_ID, f"📊 <b>AI విశ్లేషణ - పేజీ: {page}/{total_pages}</b>\n\n" + safe_html_text(response_text), parse_mode='HTML')

@bot.message_handler(commands=['globalsummary'])
def global_summary(message):
    if not collected_news: return safe_send("వార్తలు లేవు.", chat_id=message.chat.id)
    res_text = safe_gemini(f"పూర్తి గ్లోబల్ మార్కెట్ సమరీ ఇవ్వండి:\n {' '.join(collected_news[-10:])}")
    safe_send(f"📊 <b>గ్లోబల్ మార్కెట్ రిపోర్ట్:</b>\n\n{res_text}", chat_id=message.chat.id)

@bot.message_handler(commands=['summaryred'])
def redbox_summary(message):
    args = message.text.split()
    hour = int(args[1]) if len(args) > 1 and args[1].isdigit() else 6
    target_time = calculate_historical_target_time(hour)
    filtered_news = [f"Title: {n['title']}" for n in rss_news_store if isinstance(n, dict) and n.get('source') == "Redbox X" and n.get('time') >= target_time]
    if not filtered_news: return bot.reply_to(message, "⚠️ Redbox వార్తలు లేవు సార్.")
    prompt = f"Process corporate flashes in Telugu format strictly. DATA:\n" + "\n".join(filtered_news[-10:])
    response_text = ask_gemini_raw(prompt)
    if response_text: send_long_message(message.chat.id, f"🚩 <b>Smart AI Insights (REDBOX)</b>\n\n" + response_text, parse_mode='HTML')

@bot.message_handler(commands=['today', 'events'])
def handle_calendar_commands(message):
    calendar_chunks = fetch_economic_calendar(days=1)
    for chunk in calendar_chunks:
        if chunk and chunk.strip():
            bot.send_message(message.chat.id, chunk, parse_mode='HTML', disable_web_page_preview=True)
            time.sleep(1)

@bot.message_handler(commands=['get', 'getx', 'getred'])
def get_news_by_time_master(message):
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit(): return
    hour = int(args[1])
    target_time = calculate_historical_target_time(hour)
    
    source_type = "REDBOX" if 'getred' in message.text else ("X" if 'getx' in message.text else "NORMAL")
    filtered = [n for n in rss_news_store if isinstance(n, dict) and n.get('time') >= target_time]
    
    if source_type == "REDBOX": filtered = [n for n in filtered if n.get('source') == "Redbox X"]
    elif source_type == "X": filtered = [n for n in filtered if n.get('type') == "X" and n.get('source') != "Redbox X"]
    else: filtered = [n for n in filtered if n.get('type') == "NORMAL"]

    filtered.sort(key=lambda x: x['time'], reverse=True)
    if not filtered: return bot.send_message(message.chat.id, "⏳ వార్తలు ఏవీ లేవు సార్.")

    for idx, n in enumerate(filtered[:15], 1):
        msg = f"<b>News #{idx}</b>\n\n{safe_html_text(n['title'])}\n\n{n['time'].strftime('%H:%M')}"
        bot.send_message(message.chat.id, msg, parse_mode='HTML', disable_web_page_preview=True)
        time.sleep(0.3)

def get_commands_list_text():
    return ("╔════════════════════════╗\n    🤖  <b>MARKET BOT COMMANDS</b>    \n╚════════════════════════╝\n\n"
            "🧠 <b>AI SMART SUMMARIES</b>\n🔹 <code>/summary [page]</code>\n🔹 <code>/globalsummary</code>\n🔹 <code>/summaryred [hour]</code>\n\n"
            "📅 <b>ECONOMIC CALENDARS</b>\n🔹 <code>/today</code>\n🔹 <code>/events</code>\n\n"
            "⏱ <b>FETCH NEWS BY HOUR</b>\n🔸 <code>/get [hour]</code>\n🔸 <code>/getx [hour]</code>\n🔸 <code>/getred [hour]</code>\n"
            "🖥️ <b>SYSTEM STATUS</b>\n🔸 <code>/ram</code> (లైవ్ మెమరీ వాడకం 📊)\n"
            "──────────────────────\n📌 <i>చంటి గారు, కమాండ్ కాపీ చేయడానికి Tap చేయండి!</i>")
    
@bot.message_handler(commands=['list'])
def list_commands(message): safe_send(get_commands_list_text(), chat_id=message.chat.id)

# ==========================================================
# ⏱️ SCHEDULER & MASTER JOBS
# ==========================================================
scheduler = BackgroundScheduler(timezone="Asia/Kolkata", job_defaults={'misfire_grace_time': 900, 'coalesce': True, 'max_instances': 3})
today_active_event_windows = []

def schedule_today_live_events():
    global today_active_event_windows
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        if res.status_code != 200: return
        events = res.json()
        today_str = datetime.now(IST).strftime('%Y-%m-%d')
        today_active_event_windows.clear()
        for item in events:
            if item.get('impact', '').lower() not in ['high', 'medium']: continue
            full_date_raw = item.get('date', '')
            if (full_date_raw.split('T')[0] if 'T' in full_date_raw else full_date_raw) != today_str: continue
            if not item.get("time", "").strip() or item.get("time", "").lower() in ["all day", "tentative"]: continue
            try:
                event_ist = datetime.fromisoformat(full_date_raw).astimezone(IST)
                today_active_event_windows.append((event_ist - timedelta(minutes=10), event_ist + timedelta(minutes=45), item.get("title", "")))
            except: continue
    except Exception as e: log(f"❌ Error in Loading Windows: {e}")

def morning_master_job():
    for chunk in fetch_economic_calendar(1):
        if chunk and chunk.strip(): bot.send_message(CHAT_ID, chunk, parse_mode='HTML', disable_web_page_preview=True)
    schedule_today_live_events()

scheduler.add_job(morning_master_job, 'cron', hour=5, minute=50)
scheduler.add_job(lambda: check_for_live_updates(), 'interval', minutes=2)
scheduler.add_job(send_market_table, 'interval', minutes=10)
scheduler.start()

try: schedule_today_live_events()
except: pass

# --- FLASK SERVER ---
app = Flask('')
@app.route('/')
def home(): return "Bot is running perfectly on Railway!"

if __name__ == "__main__":
    Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))).start()
    Thread(target=ai_worker, daemon=True).start()
    Thread(target=main_loop, daemon=True).start()
    Thread(target=fetch_normal_rss, daemon=True).start()
    Thread(target=fetch_x_rss, daemon=True).start()
    Thread(target=half_hourly_market_pulse_loop, daemon=True).start()
    
    while True:
        try: bot.infinity_polling(timeout=90, long_polling_timeout=15, skip_pending=True)
        except: time.sleep(10)
