from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from helper.helper_operator import get_data_postgres
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.bash_operator import BashOperator
import sys


# sys.path.append("/opt/airflow/modules/")
sys.path.append("/opt/airflow/modules/MIRRORING_TEMP")
import os

os.chdir('/opt/airflow/modules/MIRRORING_TEMP/')
from article_transform import main


@dag(dag_id="article_job",
     schedule_interval=timedelta(hours=1),
     start_date=datetime(year=2024, month=8, day=11, hour=9, minute=00),
     tags=["fact_article"],
     max_active_runs=1,
     catchup=False)

def transform_article():
    get_data_log_article = PythonOperator(
        task_id=f'get_data_log_article',
        python_callable=get_data_postgres,
        op_kwargs={
            'conn_id': f'article_published_db',
            'schema': f'postgres',
            'dir_save':
            f'/opt/airflow/modules/MIRRORING_TEMP/article_temp.csv',
            'query': f"""
				select  
                    id,
                    title,
                    content,
                    created_at,
                    published_at,
                    updated_at,
                    author_id
                FROM article_published WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '3 HOUR';"""})
    
    transform_article_data = PythonOperator(
        task_id=f'transform_article_data',
        python_callable=main)
    
    local_to_s3_result = LocalFilesystemToS3Operator(
        task_id='local_to_s3_result',
        aws_conn_id='s3_default',
        filename=
        f'/opt/airflow/modules/MIRRORING_TEMP/article_temp.csv',
        dest_key=f'article_temp.csv',
        dest_bucket='s3-dwh',
        replace=True)
    
    sp_cdc_deleted_at = RedshiftDataOperator(task_id='sp_cdc_deleted_at',
                database='redshift_conn',
                cluster_identifier='redshift-datawarehouse',
                db_user='admin',
                poll_interval=10,
                await_result=True,
                aws_conn_id='aws_default',
                sql="""CALL public.sp_cdc_deleted_row_article();""")
    
    delete_temporary_file = BashOperator(
       task_id=f"delete_file_server_orders",
       bash_command=f"rm /opt/airflow/modules/MIRRORING_TEMP/article_temp.csv",
    )

    
    get_data_log_article >> transform_article_data >> local_to_s3_result >> sp_cdc_deleted_at >> delete_temporary_file

dag = transform_article()
    
