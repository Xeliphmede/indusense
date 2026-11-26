from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "datas"


@pytest.fixture
def data_dir() -> Path:
    return DATA_DIR

