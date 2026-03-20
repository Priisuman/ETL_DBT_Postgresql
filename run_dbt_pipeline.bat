@echo off
echo Starting dbt Transformation...

:: 1. Move to the project directory
cd /d D:\ETL_GCP_Postgresql\etl_gcp_sql

:: 2. Activate your STABLE Python 3.13 environment
call D:\ETL_GCP_Postgresql\dbt_stable_env\Scripts\activate

:: 3. Run the dbt transformation
dbt run

:: 4. Optional: Run tests to ensure data quality
dbt test

echo Transformation Complete!
pause