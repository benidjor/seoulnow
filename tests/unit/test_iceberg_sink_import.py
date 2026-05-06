def test_iceberg_sink_module_imports():
    from flink_jobs.lib import iceberg_sink

    assert hasattr(iceberg_sink, "register_iceberg_catalog")
    assert iceberg_sink.warehouse_namespace() == "seoul"
