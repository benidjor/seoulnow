"""FastAPI app entry. uvicorn 으로 가동.

Run:
  uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000

선결 조건:
  - /etc/hosts 에 lakekeeper / minio docker hostname alias
  - Lakekeeper / MinIO 컨테이너 가동 중
  - silver/gold Iceberg table 적재 (bronze_to_silver / silver_to_gold streaming)
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.hotspots import router as hotspots_router


def create_app() -> FastAPI:
    app = FastAPI(title="Seoul Citydata API", version="0.1.0")
    # Day 7 Task 7.4 에서 Cloudflare Tunnel + Pages 도메인 추가 시 origin 좁힐 예정.
    # 현 시점은 로컬 dev (Next.js 3000 / curl) 만 → wildcard.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.include_router(hotspots_router)

    @app.get("/health")
    def health() -> dict:
        return {"ok": True}

    return app


app = create_app()
