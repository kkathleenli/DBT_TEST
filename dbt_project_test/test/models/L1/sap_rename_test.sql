{{
  config(
    materialized='table'
  )
}}

{% set sap_tables = ['MSEG', 'MKPF'] %}

SELECT *
FROM ({{sap_col_rename(sap_tables,ref('dbt_test_l0_mseg_mkpf'))}})