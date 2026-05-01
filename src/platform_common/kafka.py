"""Kafka producer factory + JSON serializer."""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from confluent_kafka import Producer

from .config import get_settings


def build_producer(client_id: str) -> Producer:
    s = get_settings()
    return Producer(
        {
            "bootstrap.servers": s.kafka_bootstrap_servers,
            "client.id": client_id,
            "compression.type": "lz4",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "batch.num.messages": 1000,
        }
    )


def produce_json(
    producer: Producer,
    topic: str,
    key: str,
    value: dict[str, Any],
    headers: Iterable[tuple[str, bytes]] | None = None,
) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
        headers=list(headers) if headers else None,
    )
