{{ config(schema='rdbms_schema',
materialized='table'
) }}
 
WITH source AS (
    SELECT DISTINCT
        stg_num.Value AS VALUE,
        stg_num.ADSH AS ADSH,
        stg_sub.CIK,
        stg_sub.FILED AS FiledDate,
        stg_sub.INSTANCE,
        stg_pre.STMT
    FROM
        {{ ref('stg_num') }} stg_num
    INNER JOIN
        {{ ref('stg_sub') }} stg_sub
        ON stg_num.ADSH = stg_sub.ADSH
    INNER JOIN
        {{ ref('stg_pre') }} stg_pre  
        ON stg_num.ADSH = stg_pre.ADSH
        AND stg_num.TAG = stg_pre.TAG  -- Ensuring correct financial statement mapping
    WHERE
        stg_pre.STMT = 'IS'  -- Filter only Cash Flow Data
),
 
final AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['dim_company.COMPANY_SK','dim_filings.FILINGS_SK','dim_date.date_sk','src.ADSH', 'src.VALUE']) }} AS COMP_FCT_SK,  -- Fact Table SK
        dim_company.COMPANY_SK,
        dim_filings.FILINGS_SK,
        dim_date.date_sk,
        src.VALUE,
        src.ADSH AS ADSH_KEY,
        'DBT_USER' AS Created_By,  -- Placeholder for tracking
        'IS' AS STMT,  -- Marking as Cash Flow Data
        CURRENT_TIMESTAMP() AS Created_DT
    FROM
        source src
    LEFT JOIN {{ ref('dim_company') }} dim_company
        ON src.CIK = dim_company.CIK
    LEFT JOIN {{ ref('dim_filings') }} dim_filings
        ON src.STMT = dim_filings.StatementType  -- Matching on Statement Type
        AND src.FiledDate = dim_filings.FiledDate  -- Explicitly using `src.FiledDate`
    LEFT JOIN {{ ref('dim_date') }} dim_date
        ON TRY_TO_DATE(src.FiledDate::STRING, 'YYYY-MM-DD') = dim_date.Full_DT  -- Explicitly using `src.FiledDate`
)
 
SELECT ROUND(SUM(fct.VALUE), 2) AS FCT_VALUE, 
       dc.COMPANY_NAME, 
       df.FILEDDATE, 
       df.STATEMENTTYPE,
       df.TAG, 
       df.UNITOFMEASURE, 
       df.VERSION
FROM final fct
INNER JOIN {{ ref('dim_company') }} dc 
    ON fct.COMPANY_SK = dc.COMPANY_SK
INNER JOIN {{ ref('dim_filings') }} df 
    ON fct.FILINGS_SK = df.FILINGS_SK
GROUP BY dc.COMPANY_NAME, df.FILEDDATE, df.STATEMENTTYPE, df.TAG, df.UNITOFMEASURE, df.VERSION