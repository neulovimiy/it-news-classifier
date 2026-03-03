import pandas as pd
import re
import pymorphy3
import nltk
from nltk.corpus import stopwords
from tqdm import tqdm

# 1. Инициализация
print(">>> Загрузка инструментов для обработки текста...")
nltk.download('stopwords')
stop_words = set(stopwords.words('russian'))
morph = pymorphy3.MorphAnalyzer()

# Ключевые слова для выбора "элиты" (1000 лучших из 3000)
keywords = {
    'Информационная безопасность': ['взлом', 'уязвимость', 'хакер', 'атака', 'безопасность', 'шифрование', 'фишинг', 'злоумышленник', 'инцидент', 'вредонос', 'защита', 'вирус'],
    'Искусственный интеллект': ['нейросеть', 'искусственный интеллект', ' ai ', ' gpt ', 'обучение', 'машинное', 'генеративный', 'llm', 'алгоритм', 'интеллект', 'модель'],
    'Разработка ПО': ['программирование', 'код ', 'разработчик', 'тестирование', 'репозиторий', 'библиотека', 'фреймворк', 'git ', 'база данных', 'бэкенд', 'фронтенд', 'webdev'],
    'Мобильные технологии и гаджеты': ['смартфон', 'экран', 'аккумулятор', 'камера', 'процессор', ' iphone ', ' android ', 'телефон', 'гаджет', 'дисплей', 'девайс']
}

def clean_and_lemma(text):
    # Очистка: оставляем только буквы
    text = re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]', ' ', str(text).lower())
    words = text.split()
    # Лемматизация и удаление стоп-слов
    res = [morph.parse(w)[0].normal_form for w in words if w not in stop_words and len(w) > 2]
    return " ".join(res)

def get_relevance_score(text, label):
    text = str(text).lower()
    return sum(text.count(word) for word in keywords[label])

# 2. Загрузка собранной базы
print(">>> Чтение базы данных (12.000 статей)...")
df = pd.read_csv('mega_raw_final_12k.csv')

# 3. Отбор лучших
print(">>> Оценка релевантности (выбираем 1000 лучших для каждой темы)...")
df['score'] = df.apply(lambda x: get_relevance_score(x['text'], x['label']), axis=1)

# Выбираем ровно по 1000 лучших в каждой категории
df_top = df.sort_values(['label', 'score'], ascending=[True, False]).groupby('label').head(1000)

# 4. Лемматизация
print(">>> Запуск лемматизации (Пункт 3 твоего ТЗ). Это займет время...")
tqdm.pandas(desc="Обработка")
df_top['text'] = df_top['text'].progress_apply(clean_and_lemma)

# Финальная чистка от дублей, которые могли появиться после лемматизации
df_final = df_top.drop_duplicates(subset=['text']).dropna()

# Выравниваем до минимального (чтобы точно было поровну)
min_size = df_final['label'].value_counts().min()
df_final = df_final.groupby('label').head(min_size)

# Сохранение финального файла для Colab
df_final.to_csv('gold_final_dataset_4k.csv', index=False, encoding='utf-8-sig')

print("\n" + "="*50)
print("ПОДГОТОВКА ЗАВЕРШЕНА!")
print(f"Создан файл: gold_final_dataset_4k.csv")
print(f"Всего строк для обучения: {len(df_final)}")
print(df_final['label'].value_counts())
print("="*50)