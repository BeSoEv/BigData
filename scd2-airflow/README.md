# ДЗ 2.3: SCD2 и исторический JOIN по расписанию

Реализация DAG для построения истории пользователей SCD2 и расчёта получателей рассылок с учётом состояния профиля на момент отправки

## Стек

- Apache Airflow 2.10.5
- Apache Spark 3.5.4
- Hadoop 3.3.4
- Java 17
- RustFS 1.0.4
- PySpark, Docker Compose

## Запуск

Создать конфигурацию:

```bash
cp .env.example .env
```

Запустить стенд:

```bash
docker compose up -d --build
```

Проверить состояние сервисов:

```bash
docker compose ps -a
```

Дождаться состояния `healthy` у PostgreSQL, RustFS, Airflow Webserver и Scheduler.

**Airflow:** http://localhost:8080  
Логин и пароль: `student` / `student`

**RustFS Console:** http://localhost:9001

## Проверка подключения к S3

```bash
docker compose run --rm --no-deps tools \
  python /opt/airflow/scripts/check_environment.py
```

## DAG

DAG: `scd2_campaign_recipients`

Последовательность задач:

```text
build_user_scd2 → build_campaign_recipients
```

Расписание: ежедневно в 00:00 UTC.

Используются три интервала:

| Дата расчёта | Начало интервала | Конец интервала |
|---|---|---|
| 2026-01-23 | 2026-01-23 00:00 | 2026-01-24 00:00 |
| 2026-01-24 | 2026-01-24 00:00 | 2026-01-25 00:00 |
| 2026-01-25 | 2026-01-25 00:00 | 2026-01-26 00:00 |

Все три scheduled-запуска успешно выполнены.

## Данные и результаты

Используется набор `data/demo`:
- 6 000 событий;
- 100 пользователей;
- seed=42

Исходные данные в S3:

```text
s3a://hw23/raw/demo/events/
s3a://hw23/raw/demo/campaigns.csv
```

Результаты SCD2:

```text
s3a://hw23/user_scd2/snapshot_date=YYYY-MM-DD/
```

Результаты исторического JOIN:

```text
s3a://hw23/campaign_recipients/calculation_date=YYYY-MM-DD/
```

Формат хранения — Parquet.

## Проверки

Запуск проверок:

```bash
docker compose run --rm --no-deps --entrypoint python tools \
  /opt/airflow/scripts/validate_results.py
```

Проверяются:
- корректность SCD2 и временных интервалов;
- отсутствие дубликатов;
- корректность исторического JOIN;
- метаполя и связь с запусками Airflow;
- граничные случаи JOIN;
- корректность повторного расчёта;
- неизменность остальных партиций

Полный вывод проверок сохранён в:

```text
results/validation_reports/validation_2026-11-20_17-41-05.txt
```

## Материалы сдачи

В репозитории представлены:

- `results/user_scd2/` — выгрузки таблицы SCD2 из S3 за три даты;
- `results/campaign_recipients/` — выгрузки витрины получателей из S3 за три даты;
- `results/validation_reports/validation_2026-11-20_17-41-05.txt` — отчёт с результатами проверок;
- `results/validation_reports/report.pdf` — отчёт со скриншотами Airflow, результатами расчётов и подтверждением повторного запуска

Выгрузки таблиц сохранены в формате Parquet с сохранением структуры партиций. Они позволяют проверить результаты без подключения к RustFS.

## Повторный расчёт

Обе задачи DAG Run за 23.01 были повторно выполнены через Clear в Airflow.

Результаты за 23.01 перезаписаны. Партиции за 24 и 25 января остались неизменными, включая метаполя. Совпадение бизнес-результатов до и после повторного расчёта подтверждено проверками.

## Остановка

Остановить контейнеры, сохранив данные:

```bash
docker compose stop
```

Удалить контейнеры, сохранив именованные тома:

```bash
docker compose down
```

Полностью удалить контейнеры и данные:

```bash
docker compose down -v
```
