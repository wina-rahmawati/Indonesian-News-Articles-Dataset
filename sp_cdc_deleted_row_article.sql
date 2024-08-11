CREATE OR REPLACE PROCEDURE public.sp_cdc_deleted_row_article() 
LANGUAGE plpgsql 
AS $$ 
BEGIN 

-- create temporary table for copy result file transform
CREATE TEMP TABLE copy_from_s3 (
    id int,
    title varchar(200),
    content varchar(max),
    created_at varchar(100),
    published_at varchar(100),
    updated_at varchar(100),
    author_id varchar(50),
    article_category_id varchar(50),
    updated_at_id varchar(100)
);

-- Copy s3 and store to temporary table copy_from_s3
COPY copy_from_s3
FROM
    's3://s3-dwh/article_temp.csv' iam_role 'your-iam-role' CSV DELIMITER ';' ACCEPTINVCHARS emptyasnull blanksasnull IGNOREHEADER 1;

-- change the data type because, the temporary table copy_from_s3 only contains varchar datatype
CREATE TEMP TABLE change_data_type (
    select
        id :: INT AS id,
        title,
        content,
        created_at :: timestamp AS created_at,
        published_at :: timestamp AS published_at,
        updated_at :: timestamp AS updated_at,
        author_id :: int AS author_id,
        article_category_id :: int AS article_category_id,
        updated_at_id :: int AS updated_at
    FROM
        copy_from_s3
);

-- Check row by id article for detected candidate will be deleted
CREATE TEMP check_row_deleted as (
    SELECT
        a.id
    from
        public.fact_article a
        LEFT join change_data_type b on a.id = b.id
    WHERE
        b.id IS NULL
);

UPDATE
    public.fact_article
SET
    eligible_deleted_row = 1,
    deleted_at = GETDATE() - INTERVAL '7 HOUR'
FROM
    check_row_deleted a
WHERE
    a.id = public.fact_article.id;

-- the id article with eligible_deleted_row=1 will be deleted on public.fact_article and backup on public.fact_article_backup
delete from
    public.fact_article_backup
WHERE
    id in (
        select
            id
        from
            public.fact_article
        where
            eligible_deleted_row = 1
    );

INSERT INTO
    public.fact_article_backup (
        id,
        title,
        content,
        created_at,
        published_at,
        updated_at,
        author_id,
        article_category_id,
        updated_at_id,
        deleted_at.process_created
    )
SELECT
    id,
    title,
    content,
    created_at,
    published_at,
    updated_at,
    author_id,
    article_category_id,
    updated_at_id,
    deleted_at,
    GETDATE() - INTERVAL '7 HOUR' AS process_created
FROM
    public.fact_article
where
    eligible_deleted_row = 1;

DELETE FROM
    public.fact_article
WHERE
    eligible_deleted_row = 1;

-- delete insert new dataset article
DELETE FROM
    public.fact_article
WHERE
    id IN (
        SELECT
            id
        FROM
            change_data_type
    );

INSERT INTO
    public.fact_article (
        id,
        title,
        content,
        created_at,
        published_at,
        updated_at,
        author_id,
        article_category_id,
        updated_at_id,
        deleted_at,
        eligible_deleted_row,
        process_created
    )
SELECT
    id,
    title,
    content,
    created_at,
    published_at,
    updated_at,
    author_id,
    article_category_id,
    updated_at_id,
    NULL AS deleted_at,
    0 AS eligible_deleted_row,
    GETDATE() - INTERVAL '7 HOUR' AS process_created
FROM
    change_data_type;

-- drop all the temporary table 
DROP TABLE copy_from_s3;
DROP TABLE check_row_deleted;
DROP TABLE change_data_type;

END;
$$
