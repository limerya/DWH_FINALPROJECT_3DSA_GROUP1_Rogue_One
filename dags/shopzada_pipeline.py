from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import cross_downstream
from datetime import timedelta
import sys

# ---------------- SCRIPTS PATH ----------------
sys.path.append("/opt/airflow/scripts")
try:
    from ingestion import main as ingest_all_data
except ImportError:
    def ingest_all_data():
        print("Ingestion script not found or failed to import")

# ---------------- DEFAULT ARGS ----------------
default_args = {
    'owner': 'shopzada_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---------------- DAG ----------------
with DAG(
    'shopzada_pipeline',
    default_args=default_args,
    description='ShopZada End-to-End DWH Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['shopzada', 'dwh', 'etl'],
    template_searchpath=['/opt/airflow/sql']
) as dag:

    # -------- TASK 1: CREATE STAGING TABLES --------
    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='shopzada_db',
        sql="staging/staging_tables.sql"
    )

    # -------- TASK 2: INGEST RAW DATA --------
    ingest_data = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_all_data
    )

    # -------- TASK 3: TRANSFORMATIONS --------
    transform_clean_1 = PostgresOperator(
        task_id='transform_clean_1',
        postgres_conn_id='shopzada_db',
        sql="transformations/transformation_1.sql"
    )
    transform_clean_2 = PostgresOperator(
        task_id='transform_clean_2',
        postgres_conn_id='shopzada_db',
        sql="transformations/transformation_2.sql"
    )
    transform_clean_3 = PostgresOperator(
        task_id='transform_clean_3',
        postgres_conn_id='shopzada_db',
        sql="transformations/transformation_3.sql"
    )
    transform_clean_4 = PostgresOperator(
        task_id='transform_clean_4',
        postgres_conn_id='shopzada_db',
        sql="transformations/transformation_4.sql"
    )

    transformation_tasks = [
        transform_clean_1,
        transform_clean_2,
        transform_clean_3,
        transform_clean_4
    ]

    # -------- TASK 4: BUILD DIMENSIONS --------
    build_dim_order = PostgresOperator(
        task_id='build_dim_order',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Order.sql"
    )
    build_dim_date = PostgresOperator(
        task_id='build_dim_date',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Date.sql"
    )
    build_dim_product = PostgresOperator(
        task_id='build_dim_product',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Product.sql"
    )
    build_dim_customer = PostgresOperator(
        task_id='build_dim_customer',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Customer.sql"
    )
    build_dim_merchant = PostgresOperator(
        task_id='build_dim_merchant',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Merchant.sql"
    )
    build_dim_staff = PostgresOperator(
        task_id='build_dim_staff',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Staff.sql"
    )
    build_dim_campaign = PostgresOperator(
        task_id='build_dim_campaign',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Dim_Campaign.sql"
    )

    run_indexing = PostgresOperator(
        task_id='run_indexing',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Index.sql"
    )

    dimension_tasks = [
        build_dim_order,
        build_dim_date,
        build_dim_product,
        build_dim_customer,
        build_dim_merchant,
        build_dim_staff,
        build_dim_campaign
    ]

    # -------- TASK 5: BUILD FACT TABLE --------
    build_fact_table = PostgresOperator(
        task_id='build_fact_order_line_items',
        postgres_conn_id='shopzada_db',
        sql="dim_fact/Fact_Table_Updated.sql"
    )

    # ---------------- DEPENDENCIES ----------------
    create_staging_tables >> ingest_data
    ingest_data >> transformation_tasks
    cross_downstream(transformation_tasks, dimension_tasks)
    dimension_tasks >> run_indexing
    run_indexing >> build_fact_table