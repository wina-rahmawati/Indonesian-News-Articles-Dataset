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


Bonus Question:
1. The ETL/ELT that you made is new, and the data that is in the database is from 2016. What should you consider?
`Answer:`: I should consider performing an initial data load and ensuring data quality by validating the data, handling duplicate values, and making sure the data is loaded without missing records.
3. What if the table design is using a `hard delete` method. That means once the data is deleted, the row is also get deleted. So the data in
the data warehouse and in the database should sync.
`Answer:` Use Change Data Capture (CDC) to check for rows that will be deleted. I would not only apply hard deletes but also implement a backup process before any data is deleted.
