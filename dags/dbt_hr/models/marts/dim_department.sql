SELECT DISTINCT
  Department
FROM {{ ref('stg_clean_table') }}
