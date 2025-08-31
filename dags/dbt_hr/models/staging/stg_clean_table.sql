SELECT 
Employee_ID,
Full_Name,
Department,
Job_Title,
PARSE_DATE('%m/%d/%Y',TRIM(Hire_Date)) AS Hire_Date,
CASE
WHEN ARRAY_LENGTH(SPLIT(Location,',')) =2 THEN TRIM(SPLIT(Location,',')[OFFSET(0)])
ELSE NULL
END AS City,
CASE
WHEN ARRAY_LENGTH(SPLIT(Location,',')) =2 THEN TRIM(SPLIT(Location,',')[OFFSET(1)])
ELSE NULL
END AS Country,
Performance,
Experience_Years,
Status,
Work_Mode,
CAST(Salary_INR AS INT64) AS Salary_INR


FROM `hr-data-470216.hr_data.hr_data`