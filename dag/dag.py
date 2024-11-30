from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os


sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from etl import load_config, JikanDataExtractor


def extract_anime_data(**context):
    """
    Task to extract anime data from Jikan API
    """
    config = load_config()
    extractor = JikanDataExtractor(config)

    # Extract anime data
    anime_df = extractor.extract_all_anime()

    # Push the DataFrame to XCom for next task
    context['ti'].xcom_push(key='anime_df', value=anime_df.to_dict())

    return len(anime_df)


def preprocess_anime_data(**context):
    """
    Task to preprocess extracted anime data
    """
    config = load_config()
    extractor = JikanDataExtractor(config)

    # Pull DataFrame from XCom
    anime_df = pd.DataFrame.from_dict(context['ti'].xcom_pull(key='anime_df'))

    # Preprocess the data
    processed_anime_df = extractor.preprocess_anime_data(df=anime_df)

    # Optional: Save to CSV
    if "output" in config and "csv_path" in config["output"]:
        processed_anime_df.to_csv(config["output"]["csv_path"], index=False)

    # Push processed DataFrame to XCom
    context['ti'].xcom_push(key='processed_anime_df', value=processed_anime_df.to_dict())

    return len(processed_anime_df)


def load_to_postgres(**context):
    """
    Task to load processed anime data to PostgreSQL
    """
    config = load_config()
    extractor = JikanDataExtractor(config)

    # Pull processed DataFrame from XCom
    processed_anime_df = pd.DataFrame.from_dict(context['ti'].xcom_pull(key='processed_anime_df'))

    # Load to PostgreSQL
    extractor.load_to_postgres(processed_anime_df)

    return len(processed_anime_df)


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'jikan_anime_data_extraction',
    default_args=default_args,
    description='Extract, Process, and Load Anime Data from Jikan API',
    schedule_interval=timedelta(days=7),  # Run weekly
    catchup=False
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_anime_data',
    python_callable=extract_anime_data,
    provide_context=True,
    dag=dag
)

preprocess_task = PythonOperator(
    task_id='preprocess_anime_data',
    python_callable=preprocess_anime_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_task >> preprocess_task >> load_task