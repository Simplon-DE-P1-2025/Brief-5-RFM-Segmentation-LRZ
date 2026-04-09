"""
Configuration runtime partagée pour les scripts ETL.

Objectif :
  - centraliser les defaults (DSN, dataset, feuilles Excel)
  - permettre l'override par variables d'environnement ou arguments
  - rester compatible avec Airflow : dépendances externes optionnelles
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


DEFAULT_DATASET_FILENAME = "online_retail_II.xlsx"
DEFAULT_LOCAL_DSN = "postgresql://rfm_user:rfm_pass@localhost:5432/rfm_db"
DEFAULT_SHEETS = ("Year 2009-2010", "Year 2010-2011")
DEFAULT_EXPECTED_RAW_ROWS = 1_067_371

_ENV_LOADED = False


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _coerce_path(value: Path | str, *, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_project_env() -> None:
    """
    Charge optionnellement `.env` si python-dotenv est disponible.

    Important : Airflow n'installe pas forcément cette dépendance dans son
    image, donc l'import doit rester best-effort.
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        _ENV_LOADED = True
        return

    env_path = project_root() / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    _ENV_LOADED = True


@dataclass(frozen=True, slots=True)
class IngestSettings:
    project_root: Path
    db_dsn: str
    xlsx_path: Path
    sheets: tuple[str, ...]
    expected_raw_rows: int = DEFAULT_EXPECTED_RAW_ROWS


def load_ingest_settings(
    *,
    db_dsn: str | None = None,
    data_path: Path | str | None = None,
    sheets: Sequence[str] | None = None,
    expected_raw_rows: int = DEFAULT_EXPECTED_RAW_ROWS,
) -> IngestSettings:
    load_project_env()

    root = project_root()
    resolved_data_path = (
        _coerce_path(data_path, base_dir=root)
        if data_path is not None
        else _coerce_path(
            os.getenv("DATA_PATH", root / "data" / DEFAULT_DATASET_FILENAME),
            base_dir=root,
        )
    )
    resolved_dsn = db_dsn or os.getenv("RFM_DB_DSN", DEFAULT_LOCAL_DSN)
    resolved_sheets = tuple(sheets) if sheets is not None else DEFAULT_SHEETS
    if not resolved_sheets:
        raise ValueError(
            "La configuration d'ingestion doit contenir au moins une feuille Excel."
        )

    return IngestSettings(
        project_root=root,
        db_dsn=resolved_dsn,
        xlsx_path=resolved_data_path,
        sheets=resolved_sheets,
        expected_raw_rows=expected_raw_rows,
    )
