SELECT 
  Employee_ID,
  Full_Name,
  Department,
  Job_Title,
  Hire_Date,
  City,
  Country,
  Work_Mode,
  Status
FROM {{ ref('stg_clean_table') }}
