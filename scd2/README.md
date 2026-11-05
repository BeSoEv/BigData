## Структура

- `notebooks/hw21_scd2.ipynb` — решение (построение SCD2, проверки и аналитические запросы)
- `notebooks/event_log/` — сгенерированные исходные CSV-файлы
- `scripts/generate_event_log.py` — скрипт генерации исходных событий
- `docker-compose.yml` — конфигурация Docker Compose
- `Dockerfile` — сборка окружения Jupyter
- `Dockerfile.spark` — сборка окружения Spark с поддержкой S3A

## Запуск

1. Скачать архив и распаковать его
2. Перейти в корневую директорию проекта. 
3. Запустить контейнеры:

   ```bash
   docker compose up -d --build
   ```

4. Открыть Jupyter по адресу `http://localhost:8888`.
5. Открыть ноутбук `notebooks/hw21_scd2.ipynb`.
6. Выполнить ячейки последовательно.

## Данные

Для хранения данных используется S3-совместимое хранилище RustFS.

- Исходные события: `s3a://scd2-data/source/`
- Итоговая таблица SCD2: `s3a://scd2-data/scd2/users_history/`

Итоговая таблица сохраняется в формате Parquet.