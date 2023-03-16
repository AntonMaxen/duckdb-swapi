from pathlib import Path

from swapi.config import Config
from swapi.extract import extract_resources
from swapi.load import setup_db, load_data
from swapi.transform import (
    transform_dim_films,
    transform_dim_people,
    transform_dim_planets,
    transform_fact_movie_appearance,
)


def swapi_ingestion():
    """Extracts data from swapi and loads data into fact dimension model."""
    # Extract all resources listed in configuration.
    resource_data = extract_resources()

    # Transformations
    print(f"Starting transformation of resources: {list(resource_data.keys())}")
    dim_films_filename = transform_dim_films(resource_data["films"])
    dim_planets_filename = transform_dim_planets(resource_data["planets"])
    dim_people_filename = transform_dim_people(resource_data["people"])
    fact_movie_appearance_filename = transform_fact_movie_appearance(resource_data["people"])

    # Loading
    database_filepath = str(Config.working_directory / Config.database_name)

    # Setup persistent tables inside duckdb file.
    setup_db(database_filepath)

    # bulk insertion into persistent duckdb file
    load_data(
        dim_people_filename, dim_planets_filename, dim_films_filename, fact_movie_appearance_filename, database_filepath
    )

    # Remove all intermediate files.
    for file in [
        *resource_data.values(),
        dim_films_filename,
        dim_planets_filename,
        dim_people_filename,
        fact_movie_appearance_filename,
    ]:
        Path(file).unlink(missing_ok=True)

    print(f"swapi movie appearance is now available at: {str(database_filepath)} for consumption.")


if __name__ == "__main__":
    swapi_ingestion()
