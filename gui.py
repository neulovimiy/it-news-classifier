import customtkinter as ctk
import torch
import re
import pymorphy3
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Инициализация лемматизатора (для соблюдения пункта 3 твоего ТЗ)
morph = pymorphy3.MorphAnalyzer()


def preprocess_text(text):
    """Очистка и лемматизация текста перед подачей в нейросеть"""
    text = text.lower()
    text = re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]', ' ', text)
    words = text.split()
    # Приводим слова к начальной форме
    lemmas = [morph.parse(w)[0].normal_form for w in words]
    return " ".join(lemmas)


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Настройки окна
        self.title("Система классификации ИТ-контента (ВлГУ ИЗИ)")
        self.geometry("700x550")
        ctk.set_appearance_mode("light")

        # Загрузка модели на ЦП (CPU)
        print("Загрузка нейросети на процессор... Подождите.")
        self.model_path = "./my_trained_model"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
        self.model.to('cpu')
        self.model.eval()

        self.classes = ['Информационная безопасность', 'Искусственный интеллект',
                        'Разработка ПО', 'Мобильные технологии и гаджеты']

        # Элементы интерфейса
        self.title_label = ctk.CTkLabel(self, text="Интеллектуальный классификатор новостей",
                                        font=("Arial", 20, "bold"))
        self.title_label.pack(pady=20)

        self.desc_label = ctk.CTkLabel(self, text="Введите текст статьи или новости для анализа:")
        self.desc_label.pack()

        self.textbox = ctk.CTkTextbox(self, width=600, height=200, font=("Arial", 14))
        self.textbox.pack(pady=10)

        self.button = ctk.CTkButton(self, text="Определить тематику (на ЦП)",
                                    command=self.classify, fg_color="#2c3e50")
        self.button.pack(pady=20)

        self.result_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.result_frame.pack(pady=10)

        self.res_title = ctk.CTkLabel(self.result_frame, text="Результат:", font=("Arial", 16))
        self.res_title.pack(side="left", padx=5)

        self.result_text = ctk.CTkLabel(self.result_frame, text="Ожидание...",
                                        font=("Arial", 18, "bold"), text_color="#e67e22")
        self.result_text.pack(side="left")

        self.info_label = ctk.CTkLabel(self, text="Разработал: Емельянов Я.А. (гр. ИСБ-122)",
                                       font=("Arial", 10), text_color="gray")
        self.info_label.pack(side="bottom", pady=10)

    def classify(self):
        raw_text = self.textbox.get("1.0", "end-1c")
        if not raw_text.strip():
            return

        # Разбиваем на фрагменты по 100 слов
        words = raw_text.split()
        chunk_size = 100
        chunks = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]

        all_logits = []

        for chunk in chunks:
            clean_text = preprocess_text(chunk)
            inputs = self.tokenizer(clean_text, return_tensors="pt", truncation=True,
                                    max_length=128, padding=True).to('cpu')
            with torch.no_grad():
                outputs = self.model(**inputs)
                all_logits.append(outputs.logits)

        # Усреднение
        if len(all_logits) > 1:
            avg_logits = torch.mean(torch.stack(all_logits), dim=0)
        else:
            avg_logits = all_logits[0]

        probs = torch.nn.functional.softmax(avg_logits, dim=-1)
        confidence = torch.max(probs).item()
        prediction = torch.argmax(probs, dim=-1).item()

        # Логика вывода БЕЗ процентов для неясных тем
        if confidence < 0.60:
            self.result_text.configure(text="Тема не определена (низкая уверенность)", text_color="orange")
        else:
            category = self.classes[prediction]
            # Выводим проценты только если тема определена уверенно
            self.result_text.configure(text=f"{category} ({confidence * 100:.1f}%)", text_color="#27ae60")

if __name__ == "__main__":
    app = App()
    app.mainloop()