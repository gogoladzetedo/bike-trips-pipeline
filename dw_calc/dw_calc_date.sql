INSERT INTO dw.dim_date (
      id 
    , day_of_week 
    , day_of_month 
    , week 
    , month 
    , quarter 
    , year )
SELECT cast(date_id AS DATE) AS date_id
    , extract(ISODOW FROM date_id)-1 AS day_of_week
    , extract(DAY FROM date_id) AS day_of_month
    , extract(WEEK FROM date_id) AS week
    , extract(MONTH FROM date_id) AS month
    , extract(QUARTER FROM date_id) AS quarter
    , extract(YEAR FROM date_id) AS year
FROM generate_series(
    '2016-01-01',
    '2021-12-31', INTERVAL '1 day'
  )AS date_id  
WHERE
    NOT EXISTS (
        SELECT 1 
        FROM dw.dim_date AS fct 
        WHERE fct.id = cast(date_id AS DATE)
    )


INSERT INTO dw.dim_hour (id)
SELECT hour_id
FROM generate_series(
    0,
    23
  )AS hour_id  

WHERE
    NOT EXISTS (
        SELECT 1 
        FROM dw.dim_hour AS fct 
        WHERE fct.id = hour_id
    )