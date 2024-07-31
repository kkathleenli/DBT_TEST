{{
  config(
    materialized='table'
  )
}}

WITH L0_TEST
AS (
    SELECT *
    FROM SAP_BW.L0.STAGE_ZSD_C01
)

SELECT *
FROM L0_TEST