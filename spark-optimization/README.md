# Spark Optimization

Проект по выполнению домашнего задания «Оптимизация вычислений в Spark».

## Структура проекта

```text
spark-optimization/
├── data/
│   ├── task1/
│   ├── task2/
│   └── task3/
├── notebooks/
│   ├── task_1_join_strategies.ipynb
│   ├── task_2_data_skew.ipynb
│   └── task_3_user_features.ipynb
├── results/
├── scripts/
│   └── generate_event_log.py
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.spark
├── README.md
└── task.pdf
```

## Требования

- Apache Spark 3.5.3
- Docker и Docker Compose
- Jupyter Notebook

## Запуск окружения

1. Перейти в корневую директорию проекта.
2. Собрать образы и запустить контейнеры:

   ```bash
   docker compose up -d --build
   ```

3. Открыть Jupyter Notebook: http://localhost:8888.
4. Выполнять ноутбуки последовательно в соответствии с заданиями.

## Исходные данные

Исходные данные расположены в папке `data` и разделены по заданиям:

- `data/task1/` — данные для сравнения стратегий JOIN.
- `data/task2/` — данные для исследования Data Skew.
- `data/task3/` — данные для построения пользовательских признаков.

## Хранение данных

Для хранения промежуточных и итоговых данных используется S3-совместимое хранилище RustFS.

Адрес S3 API: `http://localhost:9000`.

Результаты выполнения заданий сохраняются в папке `results` и/или в RustFS.