{{
  config(
    materialized='table'
  )
}}

WITH L0_TEST
AS (
    SELECT *
    FROM SAP_BW.L0.STAGE_MSEG_MKPF
)

SELECT *
FROM L0_TEST