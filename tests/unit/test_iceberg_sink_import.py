"""iceberg_sink 모듈 import smoke + warehouse_namespace 가 settings 와 일치하는지 검증."""


def test_iceberg_sink_module_imports():
    from flink_jobs.lib import iceberg_sink
    from platform_common import get_settings

    assert hasattr(iceberg_sink, "register_iceberg_catalog")
    assert iceberg_sink.warehouse_namespace() == get_settings().iceberg_catalog_name
