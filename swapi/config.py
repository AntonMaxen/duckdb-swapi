from dataclasses import dataclass
from pathlib import Path
from typing import Collection


@dataclass(frozen=True)
class _Config:
    """General configuration for swapi ingestion"""

    base_url: str = "https://swapi.dev/api/"
    resources: Collection[str] = ("films", "planets", "people")
    working_directory: Path = Path(__file__).parent / "data"
    database_name: str = "swapi_movie_appearance.db"


Config = _Config()
