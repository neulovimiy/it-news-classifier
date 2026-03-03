import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from tqdm import tqdm
import logging

# === 1. НАСТРОЙКА ЛОГИРОВАНИЯ (Только в файл) ===
with open("parser_log.txt", "w", encoding='utf-8') as f: f.write("")
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("parser_log.txt", encoding='utf-8')])
logger = logging.getLogger(__name__)

# === 2. НАСТРОЙКИ ===
global_seen = set()
final_data = []
final_stats = []

UA_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
]

def get_soup(session, url):
    headers = {'User-Agent': random.choice(UA_LIST), 'Referer': 'https://yandex.ru/'}
    try:
        res = session.get(url, headers=headers, timeout=12)
        if res.status_code == 200: return BeautifulSoup(res.text, 'html.parser')
    except: pass
    return None

# === 3. ИНДИВИДУАЛЬНЫЕ ПАРСЕРЫ (РАБОЧИЕ КУСКИ) ===

def parse_xakep(session, label, target=1000):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  Xakep.ru      ", leave=True, unit="ст")
    while collected < target and page < 100:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://xakep.ru/category/news/page/{page}/")
        if not soup: break
        items = soup.find_all('h3', class_='entry-title') # ТВОЙ РАБОЧИЙ КОД
        for i in items:
            t = i.get_text(strip=True)
            if len(t) > 20 and t not in global_seen:
                global_seen.add(t)
                final_data.append({'text': t, 'label': label, 'site': 'Xakep.ru'})
                collected += 1
                pbar.update(1)
                last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'Xakep.ru', 'Категория': label, 'Собрано': collected})
    return collected

def parse_seclab_universal(session, label, target=1000):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  SecurityLab   ", leave=True, unit="ст")
    while collected < target and page < 80:
        if time.time() - last_added > 30: break
        url = f"https://www.securitylab.ru/news/page1_{page}.php" if page > 1 else "https://www.securitylab.ru/news/"
        soup = get_soup(session, url)
        if not soup: break
        # ТВОЙ РАБОЧИЙ КУСОК (УНИВЕРСАЛЬНЫЙ ПОИСК БЛОКОВ)
        items = soup.find_all(['article', 'div', 'a'], class_=lambda x: x and any(c in x for c in ['article', 'item', 'card', 'post']))
        for i in items:
            t_el = i.find(['h2', 'h3', 'h4', 'a', 'span'])
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 20 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'SecurityLab'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'SecurityLab', 'Категория': label, 'Собрано': collected})
    return collected

def parse_naked_universal(session, label, target=1000):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  NakedScience  ", leave=True, unit="ст")
    while collected < target and page < 60:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://naked-science.ru/article/hi-tech/page/{page}")
        if not soup: break
        # ТВОЙ РАБОЧИЙ КУСОК (УНИВЕРСАЛЬНЫЙ ПОИСК БЛОКОВ)
        items = soup.find_all(['article', 'div'], class_=lambda x: x and any(c in x for c in ['article', 'item', 'card', 'post', 'news']))
        for i in items:
            t_el = i.find(['h2', 'h3', 'a'])
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 15 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'NakedScience'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'NakedScience', 'Категория': label, 'Собрано': collected})
    return collected

def parse_habr(session, hub, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc=f"  Habr/{hub: <12}", leave=True, unit="ст")
    while collected < target and page < 150:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://habr.com/ru/hubs/{hub}/articles/page{page}/")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2') # Строго h2 для Хабра
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 18 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': f'Habr/{hub}'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': f'Habr/{hub}', 'Категория': label, 'Собрано': collected})
    return collected

def parse_antimalware(session, label, target=1000):
    collected, page, last_added = 0, 0, time.time()
    pbar = tqdm(total=target, desc="  Anti-Malware  ", leave=True, unit="ст")
    while collected < target and page < 80:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://www.anti-malware.ru/news?page={page}")
        if not soup: break
        items = soup.select('div.views-row')
        for i in items:
            t_el = i.select_one('a')
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 18 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'Anti-Malware'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'Anti-Malware', 'Категория': label, 'Собрано': collected})
    return collected

def parse_hinews(session, tag, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc=f"  Hi-News/{tag: <8}", leave=True, unit="ст")
    while collected < target and page < 80:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://hi-news.ru/tag/{tag}/page/{page}/")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2')
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 20 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'Hi-News'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': f'Hi-News/{tag}', 'Категория': label, 'Собрано': collected})
    return collected

def parse_devby(session, label, target=1000):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  DevBy         ", leave=True, unit="ст")
    while collected < target and page < 80:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://devby.io/news?page={page}")
        if not soup: break
        items = soup.select('.card__body')
        for i in items:
            t_el = i.select_one('a')
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 18 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'DevBy'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'DevBy', 'Категория': label, 'Собрано': collected})
    return collected

def parse_androidinsider(session, label, target=1000):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  AndroidInsider", leave=True, unit="ст")
    while collected < target and page < 80:
        if time.time() - last_added > 30: break
        soup = get_soup(session, f"https://androidinsider.ru/page/{page}")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2')
            if t_el:
                t = t_el.get_text(strip=True)
                if len(t) > 20 and t not in global_seen:
                    global_seen.add(t)
                    final_data.append({'text': t, 'label': label, 'site': 'AndroidInsider'})
                    collected += 1
                    pbar.update(1)
                    last_added = time.time()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'AndroidInsider', 'Категория': label, 'Собрано': collected})
    return collected

# === 4. ЗАПУСК ===
s = requests.Session()
print(">>> ЗАПУСК ФИНАЛЬНОГО СБОРА 12.000 СТАТЕЙ")

# ТЕМА 1: ИБ
print("\n[Категория: Информационная безопасность]")
c1 = 0
c1 += parse_xakep(s, "Информационная безопасность", 1000)
c1 += parse_antimalware(s, "Информационная безопасность", 1000)
c1 += parse_seclab_universal(s, "Информационная безопасность", 1000)
if c1 < 3000:
    print(f"  Добив ИБ через Хабр: {3000 - c1} шт.")
    parse_habr(s, "infosecurity", "Информационная безопасность", 3000 - c1)

# ТЕМА 2: ИИ
print("\n[Категория: Искусственный интеллект]")
c2 = 0
c2 += parse_hinews(s, "iskusstvennyj-intellekt", "Искусственный интеллект", 1000)
c2 += parse_naked_universal(s, "Искусственный интеллект", 1000)
c2 += parse_habr(s, "artificial_intelligence", "Искусственный интеллект", 1000)
if c2 < 3000:
    print(f"  Добив ИИ через Хабр: {3000 - c2} шт.")
    parse_habr(s, "machine_learning", "Искусственный интеллект", 3000 - c2)

# ТЕМА 3: ПО
print("\n[Категория: Разработка ПО]")
c3 = 0
c3 += parse_devby(s, "Разработка ПО", 1000)
c3 += parse_habr(s, "programming", "Разработка ПО", 1000)
c3 += parse_habr(s, "webdev", "Разработка ПО", 1000)
if c3 < 3000:
    parse_habr(s, "software", "Разработка ПО", 3000 - c3)

# ТЕМА 4: ГАДЖЕТЫ
print("\n[Категория: Мобильные технологии]")
c4 = 0
c4 += parse_androidinsider(s, "Мобильные технологии и гаджеты", 1000)
c4 += parse_habr(s, "smartphones", "Мобильные технологии и гаджеты", 1000)
c4 += parse_habr(s, "gadgets", "Мобильные технологии и гаджеты", 1000)
if c4 < 3000:
    parse_habr(s, "mobile_dev", "Мобильные технологии и гаджеты", 3000 - c4)

# СОХРАНЕНИЕ
df = pd.DataFrame(final_data).drop_duplicates(subset=['text'])
df.to_csv('mega_raw_final_12k.csv', index=False, encoding='utf-8-sig')

print("\n" + "=" * 60)
print(f"{'ИТОГОВЫЙ ОТЧЕТ':^60}")
print("=" * 60)
print(pd.DataFrame(final_stats).to_string(index=False))
print("-" * 60)
print(f"ИТОГО УНИКАЛЬНЫХ СТАТЕЙ: {len(df)}")