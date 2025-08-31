SELECT DISTINCT
  Job_Title,
  Department
FROM {{ ref('stg_clean_table') }}
