SELECT
  Employee_ID,
  Salary_INR,
  Experience_Years,
  Performance
FROM {{ ref('stg_clean_table') }}
