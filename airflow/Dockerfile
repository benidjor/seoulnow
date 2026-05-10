# Airflow 2.10.5 base image (Python 3.11) + 본 프로젝트 의존성.
#
# Plan 대비 변경:
#   - plan 의 base image 버전 미명시 → 2.10.5-python3.11 (constraints lock 안정).
#   - dbt-core / dbt-duckdb / pyiceberg / duckdb / boto3 추가 — DAG 안 dbt CLI
#     호출 + Iceberg lookup + DuckDB 검증 위함.
#   - pip 대신 uv resolver — pip 가 dbt-core + pyiceberg + Airflow transitive
#     deps 의 backtracking 200000+ rounds 후 ResolutionTooDeep 으로 fail
#     (실측). uv 는 같은 의존성 그래프를 빠르게 해소.
#   - Airflow 공식 constraints 를 같이 적용 — protobuf 등 Airflow 가 lock 한
#     버전 보존, 추가 패키지의 transitive 가 그 안에서 resolve.
FROM apache/airflow:2.10.5-python3.11

# uv 단일 binary 를 system path 에 복사.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

# Airflow system venv 에 install — root 권한 필요 (`/usr/local/lib/python3.11/
# site-packages` write).
USER root
COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN uv pip install --no-cache-dir --system \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /opt/airflow/requirements.txt

# dbt 전용 venv — airflow user 에 write 권한 있는 /opt/airflow 안.
# DAG 의 BashOperator 가 ${DBT_VENV_BIN}/dbt 호출.
USER airflow
COPY airflow/dbt-requirements.txt /opt/airflow/dbt-requirements.txt
RUN uv venv /opt/airflow/dbt-venv --python 3.11 && \
    uv pip install --no-cache-dir --python /opt/airflow/dbt-venv/bin/python \
    -r /opt/airflow/dbt-requirements.txt

ENV DBT_VENV_BIN=/opt/airflow/dbt-venv/bin
