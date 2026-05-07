"""Lakekeeper warehouse 등록 (멱등).

LAKEKEEPER_URL 의 management API 를 호출해 'seoul' warehouse 를
S3 (MinIO) backend 로 등록한다. 이미 있으면 skip.

Usage:
    uv run --with httpx python infra/lakekeeper/bootstrap.py
"""
from __future__ import annotations

import os
import sys

import httpx

LAKEKEEPER_URL = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181")
WAREHOUSE_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "seoul")
BUCKET = os.environ.get("ICEBERG_WAREHOUSE_BUCKET", "seoul-warehouse")
# NOTE: Lakekeeper runs inside Docker and reaches MinIO via the Docker-internal
# hostname.  Use MINIO_ENDPOINT=http://minio:9000 (default).  Override to
# http://localhost:9000 only when running Lakekeeper outside Docker.
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def ensure_server_bootstrapped(client: httpx.Client) -> None:
    """POST /management/v1/bootstrap if the server has not been bootstrapped yet.

    A fresh Lakekeeper instance returns an empty project-list until the
    one-time server bootstrap is performed (requires accept-terms-of-use=true).
    Idempotent: 2xx on first call; subsequent calls may return 409 (already
    bootstrapped) which we treat as success.
    """
    r = client.post(
        f"{LAKEKEEPER_URL}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
    )
    # v0.5.x returns 400 with type "CatalogAlreadyBootstrapped" when already done
    if r.status_code in (400, 409):
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        error_block = body.get("error")
        error_type = error_block.get("type", "") if isinstance(error_block, dict) else ""
        if "already" in error_type.lower() or r.status_code == 409:
            return  # already bootstrapped — idempotent
    if r.status_code >= 400:
        print(f"bootstrap failed: {r.status_code} {r.text}", file=sys.stderr)
        r.raise_for_status()
    print("server bootstrapped (or already was)")


def get_default_project_id(client: httpx.Client) -> str:
    r = client.get(f"{LAKEKEEPER_URL}/management/v1/project-list")
    r.raise_for_status()
    projects = r.json().get("projects", [])
    if not projects:
        raise RuntimeError("no project found in Lakekeeper")
    return projects[0]["project-id"]


# Lakekeeper v0.12 의 storage-profile schema. 핵심 필드:
# - flavor: "s3-compat" (v0.5 의 "minio" 대체. v0.12 에서 minio 는 invalid)
# - remote-signing-enabled: False — default true 면 LoadTable response 에
#   s3.signer=S3V4RestSigner 가 박혀 client (PyFlink/pyiceberg) 가 sign API
#   호출 시도 → auth disable 환경에서 SignError. false 로 강제해 client 가
#   직접 access-key 인증.
# storage-credential schema 도 v0.5 의 aws-access-key-id → v0.12 access-key-id
# 로 prefix 변경.
def _storage_profile() -> dict:
    return {
        "type": "s3",
        "bucket": BUCKET,
        "key-prefix": "warehouse",
        "endpoint": MINIO_ENDPOINT,
        "region": MINIO_REGION,
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": False,
        "remote-signing-enabled": False,
    }


def _storage_credential() -> dict:
    return {
        "type": "s3",
        "credential-type": "access-key",
        "access-key-id": MINIO_USER,
        "secret-access-key": MINIO_PASS,
    }


def get_warehouse_id(client: httpx.Client, project_id: str) -> str | None:
    r = client.get(
        f"{LAKEKEEPER_URL}/management/v1/warehouse",
        params={"project-id": project_id},
    )
    r.raise_for_status()
    for wh in r.json().get("warehouses", []):
        if wh["name"] == WAREHOUSE_NAME:
            return wh["warehouse-id"]
    return None


def create_warehouse(client: httpx.Client, project_id: str) -> None:
    payload = {
        "warehouse-name": WAREHOUSE_NAME,
        "project-id": project_id,
        "storage-profile": _storage_profile(),
        "storage-credential": _storage_credential(),
    }
    r = client.post(f"{LAKEKEEPER_URL}/management/v1/warehouse", json=payload)
    if r.status_code >= 400:
        print(f"create warehouse failed: {r.status_code} {r.text}", file=sys.stderr)
        r.raise_for_status()
    print(f"created warehouse '{WAREHOUSE_NAME}'")


def update_warehouse_storage(client: httpx.Client, warehouse_id: str) -> None:
    """기존 warehouse 의 storage-profile + credential 을 강제 동기화.

    v0.5 시절 만든 warehouse 가 v0.12 환경에서도 remote-signing-enabled=false
    설정을 갖도록 멱등 보정. spec 변경 시 main 호출만으로 일관 상태 유지.
    """
    payload = {
        "storage-profile": _storage_profile(),
        "storage-credential": _storage_credential(),
    }
    r = client.post(
        f"{LAKEKEEPER_URL}/management/v1/warehouse/{warehouse_id}/storage",
        json=payload,
    )
    if r.status_code >= 400:
        print(f"update warehouse storage failed: {r.status_code} {r.text}", file=sys.stderr)
        r.raise_for_status()
    print(f"updated warehouse '{WAREHOUSE_NAME}' storage-profile (remote-signing-enabled=false)")


def main() -> None:
    with httpx.Client(timeout=30.0) as client:
        ensure_server_bootstrapped(client)
        project_id = get_default_project_id(client)
        warehouse_id = get_warehouse_id(client, project_id)
        if warehouse_id is None:
            create_warehouse(client, project_id)
        else:
            print(f"warehouse '{WAREHOUSE_NAME}' exists ({warehouse_id}), syncing storage profile")
            update_warehouse_storage(client, warehouse_id)


if __name__ == "__main__":
    main()
