"""Oracle Cloud HTTP receiver — Edge API (Task 11.1) → Kafka user.events.v1.

REST Proxy 패턴 (CLAUDE.md §3): Cloudflare Pages Functions 가 HTTPS 로 보낸
익명 행동 이벤트 배치를 Bearer 토큰으로 인증하고, pydantic 으로 검증한 뒤
Kafka `user.events.v1` 토픽에 발행한다 (key=anon_id, header=ingest_ts).

aiokafka 는 lifespan 안에서만 import 한다 — 본 receiver 전용 컨테이너
의존성(requirements.txt)이므로 플랫폼 venv 에는 두지 않고, 단위/통합
테스트는 module-global `producer` 를 mock 으로 대체한다.
"""
from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
RECEIVER_TOKEN = os.environ["RECEIVER_TOKEN"]
TOPIC = "user.events.v1"

# 런타임에 lifespan 이 채운다. 테스트는 AsyncMock 으로 직접 주입.
producer: Any | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    from aiokafka import AIOKafkaProducer  # 컨테이너 전용 의존성 — lazy import

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(lifespan=lifespan)


class IncomingEvent(BaseModel):
    # JSON Schema (schemas/user_events_v1.json) 와 1:1 필드 매핑.
    # 런타임 검증은 pydantic, 계약/문서는 JSON Schema 가 담당.
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_ts: datetime
    anon_id: uuid.UUID
    user_id: uuid.UUID | None = None
    session_id: str | None = None
    event_type: str
    page: dict | None = None
    client: dict | None = None
    properties: dict | None = None


class EventBatch(BaseModel):
    events: list[IncomingEvent]


@app.post("/v1/events")
async def post_events(batch: EventBatch, authorization: str = Header(...)) -> dict:
    if authorization != f"Bearer {RECEIVER_TOKEN}":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token")
    if producer is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "producer not ready")

    ingest_ts = datetime.now(UTC).isoformat()
    for ev in batch.events:
        await producer.send_and_wait(
            TOPIC,
            value=ev.model_dump(mode="json"),
            key=str(ev.anon_id),
            headers=[("ingest_ts", ingest_ts.encode("utf-8"))],
        )
    return {"published": len(batch.events)}
