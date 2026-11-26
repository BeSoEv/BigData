
# DWH онлайн-кинотеатра

## Структура проекта

```text
cinema-analytics-pipeline/
├── dags/                       # Airflow DAG
├── jobs/                       # Spark-обработка данных
├── scripts/                    # Вспомогательные скрипты
├── sql/
│   ├── ddl/                    # DDL-скрипты ClickHouse
│   └── queries/                # SQL-запросы для проверки результатов
├── data/                       # Входные данные и каталог контента
├── docker/                     # Dockerfile и конфигурация сервисов
├── docker-compose.yaml          # Конфигурация инфраструктуры
├── .env.example                # Пример переменных окружения
└── README.md
```

### Основные файлы

- `dags/cinema_batch_dag.py` — DAG пакетной обработки в Airflow.
- `jobs/scd2.py` — построение SCD2-истории профилей пользователей.
- `jobs/batch_processing.py` — очистка, дедупликация, JOIN и расчёт пакетных витрин.
- `jobs/rdd_vs_df.py` — сравнение результатов Spark RDD и DataFrame.
- `jobs/spark_session.py` — создание Spark-сессии.
- `scripts/init_clickhouse.sh` — инициализация ClickHouse.
- `scripts/init_s3.py` — подготовка S3 и загрузка каталога контента.
- `scripts/archive_consumer.py` — архивирование сообщений Kafka в S3.
- `scripts/replay_kafka.py` — отправка тестовых данных в Kafka.
- `scripts/generate_data.py` — генерация тестовых данных.

## Запуск проекта

Создать файл конфигурации:

```bash
cp .env.example .env
```

Запустить инфраструктуру:

```bash
docker compose up -d --build
```

Проверить состояние контейнеров:

```bash
docker compose ps
```

## Инициализация хранилищ

Инициализировать ClickHouse:

```bash
bash scripts/init_clickhouse.sh
```

Подготовка S3 выполняется при запуске сервиса `s3-init`. Скрипт `scripts/init_s3.py` создаёт структуру хранения и загружает каталог контента.

## Загрузка данных

Установить зависимости для отправки данных:

```bash
python3 -m pip install -r scripts/requirements.txt
```

Отправить демонстрационный набор в Kafka:

```bash
python3 scripts/replay_kafka.py \
  --dataset data/demo \
  --bootstrap-servers localhost:9092 \
  --rate 100
```

Используются два Kafka-топика:

- `cinema.events` — просмотры и поисковые запросы;
- `cinema.profile_changes` — изменения профилей пользователей.

Сервис `archive-consumer` читает сообщения Kafka отдельной consumer group и сохраняет их в S3. Архив используется для пакетной обработки.

## Архитектура обработки данных

Проект включает два контура.

**Оперативный контур:**

```text
Kafka
  → ClickHouse Kafka Engine
  → Materialized View
  → Оперативные показатели
```

Данные поступают в ClickHouse и используются для расчёта поминутного количества просмотров и времени просмотра по контенту.

**Пакетный контур:**

```text
Kafka
  → archive-consumer
  → S3
  → Spark
  → Пакетные витрины ClickHouse
```

Airflow управляет пакетной обработкой и публикацией результатов.

## Пакетная обработка

Пакетная обработка запускается через Airflow DAG `cinema_batch_pipeline`.

Основные этапы:

1. Построение SCD2-истории профилей пользователей (`jobs/scd2.py`).
2. Чтение архивных данных из S3, очистка и дедупликация событий.
3. JOIN просмотров с каталогом контента.
4. Исторический JOIN с версией профиля, действовавшей на момент события.
5. Расчёт пакетных витрин (`jobs/batch_processing.py`).
6. Проверка и публикация результатов в ClickHouse.
7. Обновление границы `T` после успешной публикации.

DAG запускается по расписанию. Для ручного запуска можно указать расчётный интервал:

```bash
docker compose exec airflow-scheduler airflow dags trigger \
  --conf '{"interval_start":"2026-01-01T00:00:00Z","interval_end":"2026-01-02T00:00:00Z"}' \
  cinema_batch_pipeline
```

Для пересчёта исторического периода указываются соответствующие `interval_start` и `interval_end`.

## Пакетные витрины

В ClickHouse формируются четыре витрины:

| Витрина | Содержание |
|---|---|
| `batch_minute_metrics` | Количество просмотров и время просмотра по минутам и контенту |
| `batch_daily_metrics` | Дневные показатели по стране и тарифу пользователя |
| `batch_daily_top5` | Топ-5 контента за каждый день |
| `batch_search_frequencies` | Частоты поисковых слов за каждый день |

Для пакетных результатов сохраняются метаданные расчёта: `run_id`, `interval_start`, `interval_end`, `processed_at`.

## Сравнение RDD и DataFrame

Для проверки эквивалентности реализаций Spark используется `jobs/rdd_vs_df.py`.

Запуск:

```bash
docker compose --profile tools run --rm tools \
  python /opt/airflow/jobs/rdd_vs_df.py
```

Сравниваются частоты поисковых слов и дневной топ-5 контента на одном контрольном наборе данных из S3.

## Lambda-отчёт

Представление `lambda_report` объединяет пакетные и оперативные показатели:

- `minute < T` — данные из пакетной витрины;
- `minute >= T` — данные из оперативного контура.

Граница `T` обновляется после успешной публикации пакетных результатов.

Пример запроса:

```sql
SELECT *
FROM cinema.lambda_report
ORDER BY minute, content_id;
```

