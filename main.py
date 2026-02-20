import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Словарь категорий: Название - ссылка на Хабр
CATEGORIES = {
    'Информационная безопасность': 'infosecurity',
    'Искусственный интеллект': 'artificial_intelligence',
    'Разработка ПО': 'programming',
    'Мобильные технологии и гаджеты': 'smartphones'
}

TARGET = 1000  # Собираем с запасом, чтобы после очистки осталось много
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}


def parse_ultimate():
    all_rows = []

    for label, hub in CATEGORIES.items():
        print(f"--- Начинаю сбор: {label} ---")
        page = 1
        count = 0

        while count < TARGET:
            url = f"https://habr.com/ru/hubs/{hub}/articles/page{page}/"
            try:
                res = requests.get(url, headers=HEADERS, timeout=10)
                if res.status_code != 200:
                    print(f"Остановка на странице {page}, код {res.status_code}")
                    break

                soup = BeautifulSoup(res.text, 'html.parser')
                articles = soup.find_all('article')

                if not articles:
                    print("Статьи закончились.")
                    break

                for a in articles:
                    if count >= TARGET: break

                    # Извлекаем заголовок
                    title = a.find('h2').text.strip() if a.find('h2') else ""
                    # Извлекаем превью текста
                    body = a.find('div', class_='article-formatted-body')
                    desc = body.text.strip() if body else ""

                    full_text = f"{title}. {desc}"

                    # Фильтр: берем только качественные, длинные тексты (> 200 символов)
                    if len(full_text) > 200:
                        all_rows.append({'text': full_text, 'label': label})
                        count += 1

                print(f"Собрано {count}...")
                page += 1
                time.sleep(0.5)  # Пауза, чтобы не забанили

            except Exception as e:
                print(f"Ошибка: {e}")
                break

    df = pd.DataFrame(all_rows)

    # КРИТИЧЕСКИЙ ШАГ: Удаляем все тексты, которые попали в разные категории одновременно
    # (keep=False удаляет ВСЕ копии такого текста, оставляя только уникальные для одной темы)
    df = df.drop_duplicates(subset=['text'], keep=False)

    # Балансировка: выравниваем количество строк по минимальному классу
    min_size = df['label'].value_counts().min()
    df = df.groupby('label').head(min_size)

    # Сохраняем результат
    df.to_csv('it_news_perfect.csv', index=False, encoding='utf-8-sig')

    print("\n" + "=" * 30)
    print("ИДЕАЛЬНЫЙ ДАТАСЕТ СОЗДАН!")
    print(f"Файл: it_news_perfect.csv")
    print(f"Всего строк: {len(df)}")
    print(df['label'].value_counts())
    print("=" * 30)


if __name__ == "__main__":
    parse_ultimate()