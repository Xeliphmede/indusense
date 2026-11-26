from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "datas"


@pytest.fixture
def data_dir() -> Path:
    return DATA_DIR


@pytest.fixture
def contracts() -> dict[str, dict]:
    """Copie des contrats du dépôt, modifiable par le test."""
    from indusense.bronze.contract import load_contracts

    return load_contracts()
