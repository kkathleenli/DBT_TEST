{{
  config(
    materialized='table'
  )
}}

SELECT *
FROM ({{bw_col_rename(ref('dbt_test_l0_c01'))}})