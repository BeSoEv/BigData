
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, KafkaException, TopicPartition


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

RUNNING = True

TOPICS = [
    "cinema.events",
    "cinema.profile_changes",
]


def stop_consumer(signum, frame):
    global RUNNING
    logger.info("Получен сигнал остановки: %s", signum)
    RUNNING = False


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://rustfs:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def create_consumer():
    config = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        ),
        "group.id": os.getenv(
            "KAFKA_ARCHIVE_GROUP_ID", "cinema-archive-consumer"
        ),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }

    consumer = Consumer(config)
    consumer.subscribe(TOPICS)
    return consumer


def flush_partition(s3, consumer, bucket, partition_key, messages):
    if not messages:
        return

    topic, partition = partition_key

    first_offset = messages[0].offset()
    last_offset = messages[-1].offset()

    body = b"".join(
        message.value() + b"\n"
        for message in messages
    )

    object_key = (
        f"raw/{topic}/partition={partition:05d}/"
        f"offsets-{first_offset:020d}-{last_offset:020d}.jsonl"
    )

    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    committed = consumer.commit(
        offsets=[
            TopicPartition(topic, partition, last_offset + 1)
        ],
        asynchronous=False,
    )

    if any(item.error is not None for item in committed):
        raise KafkaException(
            next(
                item.error
                for item in committed
                if item.error is not None
            )
        )

    logger.info(
        "Архив сохранён: s3://%s/%s; сообщений: %d; offsets: %d-%d",
        bucket,
        object_key,
        len(messages),
        first_offset,
        last_offset,
    )


def main():
    global RUNNING

    signal.signal(signal.SIGTERM, stop_consumer)
    signal.signal(signal.SIGINT, stop_consumer)

    bucket = os.getenv("S3_BUCKET", "cinema")
    max_records = int(os.getenv("ARCHIVE_BATCH_SIZE", "100"))
    flush_interval = float(os.getenv("ARCHIVE_FLUSH_INTERVAL", "5"))

    if max_records < 1 or flush_interval <= 0:
        raise ValueError("Некорректные параметры архивирования")

    s3 = create_s3_client()
    consumer = create_consumer()
    buffers = defaultdict(list)
    last_flush = time.monotonic()

    logger.info("Archive consumer запущен. Topics: %s", TOPICS)

    try:
        while RUNNING:
            message = consumer.poll(timeout=1.0)

            if message is not None:
                if message.error():
                    raise KafkaException(message.error())

                if message.value() is None:
                    raise ValueError(
                        f"Получено пустое сообщение: "
                        f"{message.topic()} offset={message.offset()}"
                    )

                json.loads(message.value())

                key = (message.topic(), message.partition())
                buffers[key].append(message)

                if len(buffers[key]) >= max_records:
                    flush_partition(
                        s3, consumer, bucket, key, buffers[key]
                    )
                    buffers[key].clear()
                    last_flush = time.monotonic()

            if time.monotonic() - last_flush >= flush_interval:
                for key in list(buffers):
                    if buffers[key]:
                        flush_partition(
                            s3, consumer, bucket, key, buffers[key]
                        )
                        buffers[key].clear()

                last_flush = time.monotonic()

    finally:
        logger.info("Завершение работы: сохраняем оставшиеся сообщения")

        try:
            for key in list(buffers):
                if buffers[key]:
                    flush_partition(
                        s3, consumer, bucket, key, buffers[key]
                    )
                    buffers[key].clear()
        finally:
            consumer.close()
            logger.info("Archive consumer остановлен")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Ошибка archive consumer")
        sys.exit(1)
