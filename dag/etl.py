import logging
import time
import pandas as pd
import psycopg2
import requests
import yaml


class JikanDataExtractor:
    def __init__(self, config):
        """
        Initialize extractor with configuration

        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.base_url = "https://api.jikan.moe/v4"

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config["logging"]["level"]),
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename=config["logging"].get("file", "anime_extraction.log"),
        )
        self.logger = logging.getLogger(__name__)

    def extract_all_anime(self):
        """
        Extract all available anime from Jikan API

        Returns:
            pandas.DataFrame: Complete anime dataset
        """
        all_anime = []
        page = 1
        max_pages = self.config["api_extraction"].get("max_pages", 10)
        rate_limit_delay = self.config["api_extraction"].get("rate_limit_delay", 2)

        while True:
            try:
                # Respect API rate limits
                time.sleep(rate_limit_delay)

                response = requests.get(
                    f"{self.base_url}/anime", params={"page": page, "limit": 25}
                )
                response.raise_for_status()

                data = response.json()

                # Break if no more data
                if not data["data"]:
                    break

                # Extend anime list
                all_anime.extend(data["data"])

                # Optional page limit
                if page >= max_pages:
                    break

                page += 1

                self.logger.info(
                    f"Extracted page {page - 1}, Total anime: {len(all_anime)}"
                )

            except requests.RequestException as e:
                self.logger.error(f"Error extracting anime on page {page}: {e}")
                break

        # Convert to DataFrame
        df = pd.DataFrame(all_anime)
        return df

    def preprocess_anime_data(self, df):
        """
        Preprocess and clean anime dataset
        Args:
            df (pandas.DataFrame): Raw anime DataFrame
        Returns:
            pandas.DataFrame: Cleaned anime DataFrame
        """
        # Select relevant columns
        columns_to_keep = [
            "mal_id",
            "url",
            "title",
            "title_english",
            "type",
            "episodes",
            "status",
            "aired",
            "rating",
            "score",
            "genres",
            "themes",
        ]

        # Create a copy with selected columns
        processed_df = df[columns_to_keep].copy()

        # Handle episodes - replace null with 0
        processed_df["episodes"] = processed_df["episodes"].fillna(0)

        # Extract start and end dates with handling for ongoing series
        def process_dates(aired):
            if not isinstance(aired, dict):
                return None, None

            # Start date
            try:
                start_date = pd.to_datetime(aired.get('from')).date() if aired.get('from') else None
            except (ValueError, TypeError):
                start_date = None

            # End date handling
            try:
                end_date = pd.to_datetime(aired.get('to')).date() if aired.get('to') else "Ongoing"
            except (ValueError, TypeError):
                end_date = "Ongoing"

            return start_date, end_date

        # Apply date processing
        processed_df[["start_date", "end_date"]] = processed_df["aired"].apply(
            lambda x: pd.Series(process_dates(x))
        )

        # Extract genre and theme names
        processed_df["genre_names"] = processed_df["genres"].apply(
            lambda x: [genre["name"] for genre in x] if isinstance(x, list) else []
        )
        processed_df["theme_names"] = processed_df["themes"].apply(
            lambda x: [theme["name"] for theme in x] if isinstance(x, list) else []
        )

        # Convert genre and theme lists to strings for easier database storage
        processed_df["genre_names"] = processed_df["genre_names"].apply(
            lambda x: ", ".join(x) if x else ""
        )
        processed_df["theme_names"] = processed_df["theme_names"].apply(
            lambda x: ", ".join(x) if x else ""
        )

        # Drop original complex columns
        processed_df.drop(columns=["aired", "genres", "themes"], inplace=True)

        return processed_df

    def load_to_postgres(self, df):
        """
        Load DataFrame to PostgreSQL database using psycopg2
        Args:
            df (pandas.DataFrame): Processed anime DataFrame
        """
        try:
            # Establish database connection
            conn = psycopg2.connect(
                host=self.config["database"]["host"],
                port=self.config["database"]["port"],
                database=self.config["database"]["database"],
                user=self.config["database"]["user"],
                password=self.config["database"]["password"],
            )

            # Create a cursor
            cur = conn.cursor()

            # Create table if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS anime (
                mal_id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                title_english TEXT,
                type TEXT,
                episodes INTEGER,
                status TEXT,
                rating TEXT,
                score FLOAT,
                start_date DATE NULL,
                end_date TEXT,
                genre_names TEXT,
                theme_names TEXT
            )
            """
            cur.execute(create_table_query)

            # Prepare and execute insert statements
            insert_query = """
            INSERT INTO anime (
                mal_id, url, title, title_english, type, episodes, 
                status, rating, score, start_date, end_date,
                genre_names, theme_names
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (mal_id) DO UPDATE SET
                url = EXCLUDED.url,
                title = EXCLUDED.title,
                title_english = EXCLUDED.title_english,
                type = EXCLUDED.type,
                episodes = EXCLUDED.episodes,
                status = EXCLUDED.status,
                rating = EXCLUDED.rating,
                score = EXCLUDED.score,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                genre_names = EXCLUDED.genre_names,
                theme_names = EXCLUDED.theme_names
            """

            # Convert DataFrame to list of tuples
            data_to_insert = [tuple(row) for row in df.to_numpy()]

            # Execute batch insert
            cur.executemany(insert_query, data_to_insert)

            # Commit and close
            conn.commit()
            self.logger.info(f"Successfully loaded {len(df)} records to anime table")

        except (Exception, psycopg2.Error) as error:
            self.logger.error(f"Database loading error: {error}")
            raise
        finally:
            # Close database connections
            if conn:
                cur.close()
                conn.close()


def load_config(config_path="config.yaml"):
    """
    Load configuration from YAML file
    Args:
        config_path (str): Path to configuration file
    Returns:
        dict: Configuration dictionary
    """
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise


def main():
    # Load configuration
    config = load_config()
    # Initialize extractor
    extractor = JikanDataExtractor(config)

    try:
        # Extract all anime
        print("Extracting Anime Data...")
        anime_df = extractor.extract_all_anime()
        # Preprocess anime data
        processed_anime_df = extractor.preprocess_anime_data(df=anime_df)

        # Optional: Save to CSV
        if "output" in config and "csv_path" in config["output"]:
            processed_anime_df.to_csv(config["output"]["csv_path"], index=False)

        # Load to PostgreSQL
        extractor.load_to_postgres(processed_anime_df)
        print("Extraction and Loading Complete!")
        print(f"Total Anime Processed: {len(processed_anime_df)}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"Extraction and loading failed: {e}")


if __name__ == "__main__":
    main()
