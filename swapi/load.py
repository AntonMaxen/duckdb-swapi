import duckdb


def setup_db(database_filepath: str) -> None:
    """Drops and recreates tables every time."""

    print(f"Setting up duckdb database at: {database_filepath}")
    with duckdb.connect(database_filepath) as con:
        # Drop tables in order of relations.
        con.execute(
            """
            DROP TABLE IF EXISTS fact_movie_appearance;
            DROP TABLE IF EXISTS dim_people;
            DROP TABLE IF EXISTS dim_films;
            DROP TABLE IF EXISTS dim_planets;
        """
        )

        # Create dimension tables
        con.execute(
            """
            CREATE TABLE dim_people (
                id INT PRIMARY KEY,
                name VARCHAR,
                hair_color VARCHAR,
                skin_color VARCHAR,
                eye_color VARCHAR,
                birth_year VARCHAR,
                gender VARCHAR,
                created TIMESTAMPTZ,
                edited TIMESTAMPTZ
            );
        """
        )

        con.execute(
            """
            CREATE TABLE dim_films (
                id INT PRIMARY KEY,
                episode_id INT,
                title VARCHAR,
                opening_crawl VARCHAR,
                director VARCHAR,
                producer VARCHAR,
                release_date DATE,
                created TIMESTAMPTZ,
                edited TIMESTAMPTZ
            );
        """
        )

        con.execute(
            """
            CREATE TABLE dim_planets (
                id INT PRIMARY KEY,
                name VARCHAR,
                climate VARCHAR,
                gravity VARCHAR,
                terrain VARCHAR,
                population INT,
                rotation_period INT,
                orbital_period INT,
                diameter INT,
                surface_water INT,
                created TIMESTAMPTZ,
                edited TIMESTAMPTZ
            );
        """
        )

        # Create fact table.
        con.execute(
            """
        CREATE TABLE fact_movie_appearance (
            character_id INT,
            film_id INT,
            home_planet_id INT,
            character_height INT,
            character_mass INT,
            PRIMARY KEY (character_id, film_id),
            FOREIGN KEY (character_id) REFERENCES dim_people(id),
            FOREIGN KEY (film_id) REFERENCES dim_films(id),
            FOREIGN KEY (home_planet_id) REFERENCES dim_planets(id)
        );
        """
        )


def load_data(
    dim_people_filename: str,
    dim_planets_filename: str,
    dim_films_filename: str,
    fact_movie_appearance_filename: str,
    database_filepath: str,
) -> None:
    """loads sw api data"""

    print(f"Loading sw api data into {database_filepath}")
    # Read fact and dimensions into duckdb
    people_view = duckdb.read_parquet(dim_people_filename)
    planet_view = duckdb.read_parquet(dim_planets_filename)
    film_view = duckdb.read_parquet(dim_films_filename)
    movie_appearance_view = duckdb.read_parquet(fact_movie_appearance_filename)

    # Bulk insert all data from memory into a persistent duckdb file.
    with duckdb.connect(database_filepath) as con:
        con.begin()
        con.execute("INSERT INTO dim_people SELECT * FROM people_view;")
        con.execute("INSERT INTO dim_planets SELECT * FROM planet_view;")
        con.execute("INSERT INTO dim_films SELECT * FROM film_view;")
        con.execute("INSERT INTO fact_movie_appearance SELECT * FROM movie_appearance_view;")
        con.commit()
