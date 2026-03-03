"""
# 1. Установка
!pip install transformers[torch] datasets evaluate scikit-learn accelerate -U

# 2. Обучение
import pandas as pd
import numpy as np
import torch
import shutil
from google.colab import files
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer

# Загрузка
df = pd.read_csv('gold_final_dataset_4k.csv').dropna()
label_map = {label: i for i, label in enumerate(df['label'].unique())}
df['label'] = df['label'].map(label_map)

# Модель
model_name = "sberbank-ai/ruBert-base"
tokenizer = AutoTokenizer.from_pretrained(model_name)

# Разделение 95% / 5%
train_texts, test_texts, train_labels, test_labels = train_test_split(
    df['text'].values.astype(str), df['label'].values, test_size=0.05, random_state=42, stratify=df['label']
)

def tokenize_function(texts):
    return tokenizer(list(texts), padding="max_length", truncation=True, max_length=256)

train_encodings = tokenize_function(train_texts)
test_encodings = tokenize_function(test_texts)

class NewsDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels
    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx])
        return item
    def __len__(self): return len(self.labels)

model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=len(label_map))

# Метрики
def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    acc = accuracy_score(labels, predictions)
    p, r, f1, _ = precision_recall_fscore_support(labels, predictions, average='weighted')
    return {"accuracy": acc, "f1": f1, "precision": p, "recall": r}

# Настройки для 98% точности
total_steps = (len(train_texts) // 16) * 6
training_args = TrainingArguments(
    output_dir='./results',
    learning_rate=2e-5,
    per_device_train_batch_size=16,
    num_train_epochs=6,
    eval_strategy="epoch",
    save_strategy="no",
    warmup_steps=int(total_steps * 0.1),
    label_smoothing_factor=0.05,
    report_to="none"
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=NewsDataset(train_encodings, train_labels),
    eval_dataset=NewsDataset(test_encodings, test_labels),
    compute_metrics=compute_metrics
)

print("\n>>> ЗАПУСК ОБУЧЕНИЯ НА 4000 ЭЛИТНЫХ СТАТЬЯХ...")
trainer.train()

# Сохранение и скачивание
model.save_pretrained("my_trained_model")
tokenizer.save_pretrained("my_trained_model")
shutil.make_archive('my_trained_model', 'zip', 'my_trained_model')
files.download('my_trained_model.zip')
"""