# Indonesian-News-Articles-Dataset

Dataset from:
https://www.kaggle.com/datasets/alvonsukardi/indonesian-news-dataset-2/data

Steps:
1. Run `initial_load.py` for data preparation and the initial load.
2. After the initial load, assume the data has been stored in the database (AWS Redshift). Add the following columns:
    - `deleted_at` (timestamp without time zone) for marking when a row is deleted.
    - `eligible_deleted_row` (int) as a flag indicating whether a row should be deleted.
    - `process_created` (timestamp without time zone) to record when the data was inserted.
3. Set up the ETL pipeline using `scheduled_job.py` with Airflow.
4. Assume the connection for transactional data is `article_published_db`, and the connection for the data warehouse (DWH) in Redshift is `redshift_conn`.
