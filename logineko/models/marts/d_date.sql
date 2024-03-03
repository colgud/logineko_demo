{{ 
    config
    (
      materialized = "table",
      alias = "D_DATE",
      schema = 'DWH',
      tags = ["DIMENSION"]
    ) 
}}

SELECT
    DATEADD('day', seq4(), '1900-01-01')::DATE as date_sid,
    date_sid as business_key,
    YEAR(business_key) AS year,
    QUARTER(business_key) AS quarter,
    MONTH(business_key) AS month,
    DAY(business_key) AS day_of_month,
    DAYOFWEEK(business_key) AS day_of_week,
    WEEKOFYEAR(business_key) AS week_of_year,
    IFF(DAYOFWEEK(business_key) IN (6, 7), TRUE, FALSE) AS is_weekend,
    DAYNAME(business_key) AS day_name, 
    MONTHNAME(business_key) AS month_name
FROM TABLE(GENERATOR(ROWCOUNT => 73414)) v,
LATERAL (SELECT DATEADD('day', seq4(), '1900-01-01') AS d FROM TABLE(GENERATOR(ROWCOUNT => 1)))
ORDER BY business_key