import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from tqdm import tqdm
import logging
import os

# === 1. НАСТРОЙКА ЛОГИРОВАНИЯ ===
with open("parser_log.txt", "w", encoding='utf-8') as f: f.write("")
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler("parser_log.txt", encoding='utf-8')])
logger = logging.getLogger(__name__)

# === 2. НАСТРОЙКИ ===
LIMIT_PER_SITE = 1000
TOTAL_TARGET = 3000
FILE_NAME = 'mega_raw_final_12k_full.csv'

global_seen = set()
final_data = []
final_stats = []

UA_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
]

# === 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def save_to_file():
    """Мгновенная запись в файл"""
    if final_data:
        df = pd.DataFrame(final_data).drop_duplicates(subset=['text'])
        df.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')

def get_soup(session, url):
    headers = {'User-Agent': random.choice(UA_LIST), 'Referer': 'https://yandex.ru/'}
    try:
        res = session.get(url, headers=headers, timeout=15)
        if res.status_code == 200: return BeautifulSoup(res.text, 'html.parser')
    except: pass
    return None

def get_full_text(session, url, selectors):
    soup = get_soup(session, url)
    if not soup: return ""
    for sel in selectors:
        content = soup.select_one(sel)
        if content:
            return content.get_text(separator=' ', strip=True)[:1000]
    return ""

# === 4. ИНДИВИДУАЛЬНЫЕ ПАРСЕРЫ ===

def parse_xakep(session, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  Xakep.ru      ", leave=True, unit="ст")
    while collected < target and page < 150:
        if time.time() - last_added > 60: break
        soup = get_soup(session, f"https://xakep.ru/category/news/page/{page}/")
        if not soup: break
        items = soup.find_all('h3', class_='entry-title')
        for i in items:
            link_el = i.find('a')
            if link_el:
                title = i.get_text(strip=True)
                link = link_el['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['.entry-content', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'Xakep.ru'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'Xakep.ru', 'Категория': label, 'Собрано': collected})
    return collected

def parse_seclab(session, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  SecurityLab   ", leave=True, unit="ст")
    while collected < target and page < 150:
        if time.time() - last_added > 40: break
        url = f"https://www.securitylab.ru/news/page1_{page}.php" if page > 1 else "https://www.securitylab.ru/news/"
        soup = get_soup(session, url)
        if not soup: break
        items = soup.select('.article-card')
        for i in items:
            details = i.select_one('.article-card-details')
            link_el = i if i.name == 'a' else (details.find('a', href=True) if details else i.find('a', href=True))
            if link_el and link_el.has_attr('href'):
                title = link_el.get_text(strip=True)
                link = link_el['href']
                if not link.startswith('http'): link = "https://www.securitylab.ru" + link
                if title not in global_seen:
                    body = get_full_text(session, link, ['.articl-text', '.article-content', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'SecurityLab'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'SecurityLab', 'Категория': label, 'Собрано': collected})
    return collected

def parse_antimalware(session, label, target):
    collected, page, last_added = 0, 0, time.time()
    pbar = tqdm(total=target, desc="  Anti-Malware  ", leave=True, unit="ст")
    while collected < target and page < 150:
        if time.time() - last_added > 40: break
        soup = get_soup(session, f"https://www.anti-malware.ru/news?page={page}")
        if not soup: break
        items = soup.select('div.views-row')
        for i in items:
            link_el = i.select_one('a')
            if link_el:
                title = link_el.get_text(strip=True)
                link = "https://www.anti-malware.ru" + link_el['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['.field-name-body', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'Anti-Malware'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'Anti-Malware', 'Категория': label, 'Собрано': collected})
    return collected

def parse_habr(session, hub, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc=f"  Habr/{hub: <12}", leave=True, unit="ст")
    while collected < target and page < 200:
        if time.time() - last_added > 40:
            page += 5
            last_added = time.time()
            continue
        soup = get_soup(session, f"https://habr.com/ru/hubs/{hub}/articles/page{page}/")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2')
            if t_el and t_el.find('a'):
                title = t_el.get_text(strip=True)
                link = "https://habr.com" + t_el.find('a')['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['.tm-article-body', '.article-formatted-body'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': f'Habr/{hub}'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': f'Habr/{hub}', 'Категория': label, 'Собрано': collected})
    return collected

def parse_hinews(session, tag, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc=f"  Hi-News/{tag: <8}", leave=True, unit="ст")
    while collected < target and page < 100:
        if time.time() - last_added > 40: break
        soup = get_soup(session, f"https://hi-news.ru/tag/{tag}/page/{page}/")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2')
            if t_el and t_el.find('a'):
                title = t_el.get_text(strip=True)
                link = t_el.find('a')['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['.post-content', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'Hi-News'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': f'Hi-News/{tag}', 'Категория': label, 'Собрано': collected})
    return collected

def parse_naked(session, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  NakedScience  ", leave=True, unit="ст")
    while collected < target and page < 100:
        if time.time() - last_added > 40: break
        soup = get_soup(session, f"https://naked-science.ru/article/hi-tech/page/{page}")
        if not soup: break
        items = soup.find_all(['article', 'div'], class_=lambda x: x and any(c in x for c in ['article', 'item', 'card', 'post', 'news']))
        for i in items:
            t_el = i.find(['h2', 'h3', 'a'])
            link_el = i.find('a', href=True)
            if t_el and link_el:
                title = t_el.get_text(strip=True)
                link = link_el['href'] if link_el['href'].startswith('http') else "https://naked-science.ru" + link_el['href']
                if len(title) > 15 and title not in global_seen:
                    body = get_full_text(session, link, ['.body', '.post-content', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'NakedScience'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'NakedScience', 'Категория': label, 'Собрано': collected})
    return collected

def parse_devby(session, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  DevBy         ", leave=True, unit="ст")
    while collected < target and page < 100:
        if time.time() - last_added > 40: break
        soup = get_soup(session, f"https://devby.io/news?page={page}")
        if not soup: break
        items = soup.select('.card__body')
        for i in items:
            t_el = i.select_one('a')
            if t_el:
                title = t_el.get_text(strip=True)
                link = "https://devby.io" + t_el['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['.article__body', 'article'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'DevBy'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'DevBy', 'Категория': label, 'Собрано': collected})
    return collected

def parse_androidinsider(session, label, target):
    collected, page, last_added = 0, 1, time.time()
    pbar = tqdm(total=target, desc="  AndroidInsider", leave=True, unit="ст")
    while collected < target and page < 100:
        if time.time() - last_added > 40: break
        soup = get_soup(session, f"https://androidinsider.ru/page/{page}")
        if not soup: break
        items = soup.select('article')
        for i in items:
            t_el = i.find('h2')
            if t_el and t_el.find('a'):
                title = t_el.get_text(strip=True)
                link = t_el.find('a')['href']
                if title not in global_seen:
                    body = get_full_text(session, link, ['article', '.post-content'])
                    if body:
                        global_seen.add(title)
                        final_data.append({'text': f"{title}. {body}", 'label': label, 'site': 'AndroidInsider'})
                        collected += 1
                        pbar.update(1)
                        last_added = time.time()
                        save_to_file()
            if collected >= target: break
        page += 1
    pbar.close()
    final_stats.append({'Сайт': 'AndroidInsider', 'Категория': label, 'Собрано': collected})
    return collected

# === 5. ЗАПУСК ГЛОБАЛЬНОГО СБОРА ===
s = requests.Session()
print(">>> ЗАПУСК ФИНАЛЬНОГО СБОРА 12.000 СТАТЕЙ (ПОЛНЫЕ ТЕКСТЫ)")

# --- КАТЕГОРИЯ 1: ИБ ---
print("\n[Категория: Информационная безопасность]")
c1 = 0
c1 += parse_xakep(s, "Информационная безопасность", LIMIT_PER_SITE)
c1 += parse_antimalware(s, "Информационная безопасность", LIMIT_PER_SITE)
c1 += parse_seclab(s, "Информационная безопасность", LIMIT_PER_SITE)
if c1 < TOTAL_TARGET:
    parse_habr(s, "infosecurity", "Информационная безопасность", TOTAL_TARGET - c1)

# --- КАТЕГОРИЯ 2: ИИ ---
print("\n[Категория: Искусственный интеллект]")
c2 = 0
c2 += parse_hinews(s, "iskusstvennyj-intellekt", "Искусственный интеллект", LIMIT_PER_SITE)
c2 += parse_naked(s, "Искусственный интеллект", LIMIT_PER_SITE)
c2 += parse_habr(s, "artificial_intelligence", "Искусственный интеллект", LIMIT_PER_SITE)
if c2 < TOTAL_TARGET:
    parse_habr(s, "machine_learning", "Искусственный интеллект", TOTAL_TARGET - c2)

# --- КАТЕГОРИЯ 3: ПО ---
print("\n[Категория: Разработка ПО]")
c3 = 0
c3 += parse_devby(s, "Разработка ПО", LIMIT_PER_SITE)
c3 += parse_habr(s, "programming", "Разработка ПО", LIMIT_PER_SITE)
c3 += parse_habr(s, "webdev", "Разработка ПО", LIMIT_PER_SITE)
if c3 < TOTAL_TARGET:
    parse_habr(s, "software", "Разработка ПО", TOTAL_TARGET - c3)

# --- КАТЕГОРИЯ 4: ГАДЖЕТЫ ---
print("\n[Категория: Мобильные технологии]")
c4 = 0
c4 += parse_androidinsider(s, "Мобильные технологии и гаджеты", LIMIT_PER_SITE)
c4 += parse_habr(s, "smartphones", "Мобильные технологии и гаджеты", LIMIT_PER_SITE)
c4 += parse_habr(s, "gadgets", "Мобильные технологии и гаджеты", LIMIT_PER_SITE)
if c4 < TOTAL_TARGET:
    parse_habr(s, "mobile_dev", "Мобильные технологии и гаджеты", TOTAL_TARGET - c4)

# ФИНАЛЬНОЕ СОХРАНЕНИЕ
save_to_file()

print("\n" + "=" * 60)
print(f"{'ИТОГОВЫЙ ОТЧЕТ':^60}")
print("=" * 60)
print(pd.DataFrame(final_stats).to_string(index=False))
print("-" * 60)
print(f"ВСЕГО УНИКАЛЬНЫХ СТАТЕЙ СОБРАНО: {len(pd.DataFrame(final_data).drop_duplicates(subset=['text']))}")