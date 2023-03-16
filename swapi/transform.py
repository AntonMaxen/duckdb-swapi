import datetime

import duckdb

from swapi.config import Config


def transform_dim_films(extracted_films_filename: str) -> str:
    """Transformation of films dimension."""

    current_timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filepath = Config.working_directory / f"transform__dim_films__{current_timestring}.parquet"

    # film_data is used within duck_db query, probably read through locals.
    film_data = duckdb.read_json(extracted_films_filename)
    films_view = duckdb.query(
        """
        SELECT 
            split_part(rtrim(url, '/'), '/', -1)::int as id,
            episode_id::int as episode_id,
            title,
            opening_crawl,
            director,
            producer,
            release_date,
            created::timestamptz as created,
            edited::timestamptz as edited
        FROM film_data
    """
    )

    films_view.to_parquet(str(output_filepath))

    return str(output_filepath)


def transform_dim_people(extracted_people_filename: str) -> str:
    """Transformation of people dimension."""

    current_timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filepath = Config.working_directory / f"transform__dim_people__{current_timestring}.parquet"

    # people_data is used within duck_db query, probably read through locals.
    people_data = duckdb.read_json(extracted_people_filename)
    people_view = duckdb.query(
        """
    SELECT 
        split_part(rtrim(url, '/'), '/', -1)::int as id,
        name,
        hair_color,
        skin_color,
        eye_color,
        birth_year,
        gender,
        created::timestamptz as created,
        edited::timestamptz as edited
    FROM people_data
    """
    )

    people_view.to_parquet(str(output_filepath))

    return str(output_filepath)


def transform_dim_planets(extracted_planets_filename: str) -> str:
    """Transformation of planets dimension."""

    current_timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filepath = Config.working_directory / f"transform__dim_planets__{current_timestring}.parquet"

    # planet_data is used within duck_db query, probably read through locals.
    planet_data = duckdb.read_json(extracted_planets_filename)
    planets_view = duckdb.query(
        """
        SELECT
            split_part(rtrim(url, '/'), '/', -1)::int as id,
            name,
            climate,
            gravity,
            terrain,
            try_cast(population as int) as population,
            try_cast(rotation_period as int) as rotation_period,
            try_cast(orbital_period as int) as orbital_period,
            try_cast(diameter as int) as diameter,
            try_cast(surface_water as int) as surface_water,
            created::timestamptz as created,
            edited::timestamptz as edited
        FROM planet_data
    """
    )

    planets_view.to_parquet(str(output_filepath))

    return str(output_filepath)


def transform_fact_movie_appearance(extracted_people_filename: str) -> str:
    """Transformation of movie appearance fact table."""

    current_timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filepath = Config.working_directory / f"transform__fact__{current_timestring}.parquet"

    # people_data is used within duck_db query, probably read through locals.
    people_data = duckdb.read_json(extracted_people_filename)
    movie_appearance_view = duckdb.query(
        """
    SELECT 
        split_part(rtrim(url, '/'), '/', -1)::int as character_id,
        split_part(rtrim(unnest(films), '/'), '/', -1)::int as film_id,
        split_part(rtrim(homeworld, '/'), '/', -1)::int as home_planet_id,
        try_cast(height as int) as character_height,
        try_cast(mass as int) as character_mass,
    FROM people_data;
    """
    )

    movie_appearance_view.to_parquet(str(output_filepath))

    return str(output_filepath)
