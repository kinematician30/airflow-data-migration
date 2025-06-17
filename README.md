# Jikan Anime Data Extractor

This project provides a robust Python solution for extracting, preprocessing, and loading anime data from the Jikan API (MyAnimeList unofficial API) into a PostgreSQL database. It's designed for data enthusiasts, analysts, or anyone looking to build a dataset of anime information.

## Features

* **Data Extraction:** Fetches anime data from the Jikan API, handling pagination and respecting API rate limits.
* **Data Preprocessing:** Cleans and transforms the raw JSON data into a structured Pandas DataFrame, extracting relevant fields like genre and theme names, and handling date formats.
* **Database Loading:** Efficiently loads the processed data into a PostgreSQL database, including table creation and conflict resolution for updates.
* **Configurable:** Uses a `config.yaml` file for easy management of API parameters, database credentials, and logging settings.
* **Logging:** Implements detailed logging for monitoring the extraction and loading process and debugging errors.

---

## Getting Started

Follow these steps to get your local copy up and running.

### Prerequisites

Before you begin, ensure you have the following installed:

* **Python 3.8+**
* **PostgreSQL:** A running PostgreSQL instance where you can create a database and tables.

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/your-repo-name.git
    cd your-repo-name
    ```

2.  **Install dependencies:**

    ```bash
    pip install pandas psycopg2-binary requests pyyaml
    ```

### Configuration

Create a `config.yaml` file in the root directory (or adjust the `load_config` function's `config_path` accordingly) with your settings:

```yaml
database:
  host: your_db_host
  port: 5432
  database: your_db_name
  user: your_db_user
  password: your_db_password

api_extraction:
  max_pages: 50 # Optional: Maximum number of pages to extract. Remove or set to a very high number for all data.
  rate_limit_delay: 2 # Delay in seconds between API requests to respect rate limits

logging:
  level: INFO # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
  file: anime_extraction.log # Optional: Log file path

output:
  csv_path: data/processed_anime.csv # Optional: Path to save the processed data as a CSV file
```

---

## Usage

Once configured, simply run the `main.py` script:

```bash
python main.py
```

The script will:

1.  Load the configuration from `config.yaml`.
2.  Connect to the Jikan API and extract anime data page by page.
3.  Preprocess the extracted data.
4.  (Optionally) Save the processed data to a CSV file.
5.  Load the processed data into your PostgreSQL database.

---

## Database Schema

The script automatically creates an `anime` table in your PostgreSQL database with the following schema:

| Column Name     | Data Type | Description                                        |
| :-------------- | :-------- | :------------------------------------------------- |
| `mal_id`        | `INTEGER` | MyAnimeList ID (Primary Key)                       |
| `url`           | `TEXT`    | URL on MyAnimeList                                 |
| `title`         | `TEXT`    | Anime title in original language                   |
| `title_english` | `TEXT`    | Anime title in English                             |
| `type`          | `TEXT`    | Type of anime (e.g., TV, Movie, OVA)               |
| `episodes`      | `INTEGER` | Number of episodes                                 |
| `status`        | `TEXT`    | Airing status (e.g., Finished Airing, Currently Airing) |
| `rating`        | `TEXT`    | Audience rating (e.g., PG-13, R)                   |
| `score`         | `FLOAT`   | MyAnimeList score                                  |
| `start_date`    | `DATE`    | Start airing date                                  |
| `end_date`      | `TEXT`    | End airing date (can be "Ongoing")                 |
| `genre_names`   | `TEXT`    | Comma-separated list of genres                     |
| `theme_names`   | `TEXT`    | Comma-separated list of themes                     |

---

## Error Handling and Logging

The script includes basic error handling for API requests and database operations. All significant events and errors are logged to the console and, if configured, to `anime_extraction.log`. You can adjust the logging level in `config.yaml`.

---

## Contributing

Feel free to fork this repository, open issues, or submit pull requests.

---
