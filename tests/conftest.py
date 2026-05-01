import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def hotspot_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_hotspot_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def subway_sample() -> dict:
    return json.loads((FIXTURE_DIR / "seoul_subway_sample.json").read_text(encoding="utf-8"))
