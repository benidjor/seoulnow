"""Day 9: 단순 운영 비용 모델 (월 추정).

본 모델은 정확한 invoice 가 아니라 운영 추정. 모든 가정을 명시적으로 출력해
검토 가능하게 한다.
"""

from __future__ import annotations

COST = {
    "oracle_always_free": 0.00,  # ARM Ampere A1 4 vCPU 24GB
    "oracle_object_storage_10gb": 0.00,
    "cloudflare_pages": 0.00,
    "cloudflare_workers_free": 0.00,
    "cloudflare_d1_free": 0.00,
    "cloudflare_tunnel": 0.00,
    "domain_optional_per_year": 10.00,
    "seoul_openapi": 0.00,
}


def main() -> None:
    monthly = 0.0
    print("== monthly cost (USD, conservative) ==")
    for k, v in COST.items():
        if k.endswith("_per_year"):
            m = v / 12.0
            print(f"  {k:40s} : {m:>6.2f}  ({v}/year)")
            monthly += m
        else:
            print(f"  {k:40s} : {v:>6.2f}")
            monthly += v
    print(f"  {'TOTAL':40s} : {monthly:>6.2f}")
    print()
    print("note: 1번 프로젝트 (레시핑) 대비 75% 인프라비 절감 흐름의 연속.")
    print("      도메인 미사용 시 월 $0. 도메인 사용 시 월 약 $0.83.")


if __name__ == "__main__":
    main()
