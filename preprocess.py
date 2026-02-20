import pandas as pd
import re

# Загружаем твой основной датасет
df = pd.read_csv('it_news_perfect.csv')

# Самые жесткие маркеры для каждой темы
gold_keywords = {
    'Информационная безопасность': ['взлом', 'уязвимость', 'хакер', 'атака', 'безопасность', 'шифрование', 'антивирус',
                                    'фишинг', 'злоумышленник', 'инцидент'],
    'Искусственный интеллект': ['нейросеть', 'искусственный интеллект', ' ai ', ' gpt ', 'обучение модели',
                                'машинное обучение', 'генеративный', 'llm', 'алгоритм'],
    'Разработка ПО': ['программирование', 'код ', 'разработчик', 'тестирование', 'репозиторий', 'библиотека',
                      'фреймворк', 'git ', 'база данных', 'бэкенд'],
    'Мобильные технологии и гаджеты': ['смартфон', 'экран', 'аккумулятор', 'камера', 'процессор', ' iphone ',
                                       ' android ', 'телефон', 'гаджет', 'дисплей']
}


def is_gold(row):
    text = str(row['text']).lower()
    label = row['label']

    # 1. Проверяем наличие сильных маркеров СВОЕЙ темы
    has_own_marker = any(word in text for word in gold_keywords[label])

    # 2. Проверяем отсутствие маркеров ДРУГИХ тем (исключаем путаницу)
    has_other_marker = False
    for other_label, words in gold_keywords.items():
        if other_label != label:
            if any(word in text for word in words):
                has_other_marker = True
                break

    # Оставляем только "чистые" примеры без примеси других тем
    return has_own_marker and not has_other_marker


print(f"Строк до фильтрации: {len(df)}")
df_gold = df[df.apply(is_gold, axis=1)]

# Оставляем по 300 лучших строк на каждый класс (всего 1200)
df_final = df_gold.groupby('label').head(300).reset_index(drop=True)

df_final.to_csv('gold_it_news.csv', index=False, encoding='utf-8-sig')
print(f"Золотой датасет готов! Всего строк: {len(df_final)}")
print(df_final['label'].value_counts())