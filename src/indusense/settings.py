"""Configuration de l'application, lue dans `.env` (le même fichier que docker compose)."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL, make_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # src/indusense/settings.py -> racine du projet


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", env_file_encoding="utf-8", extra="ignore")

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "indusense_db"
    db_user: str = "indusense_user"
    db_password: str = "dbpassword"
    database_url: str | None = None  # prioritaire sur DB_* si renseignée

    def url(self, database: str | None = None) -> URL:
        """URL SQLAlchemy (driver psycopg 3) ; database remplace le nom de la base (ex. base de test)."""
        if self.database_url:
            url = make_url(self.database_url)
        else:
            url = URL.create(
                "postgresql+psycopg",
                username=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
            )
        return url.set(database=database) if database else url


@lru_cache
def get_settings() -> Settings:
    return Settings()
