{{
  config(
    materialized='table'
  )
}}

WITH L0_TEST
AS (
    SELECT *
    FROM TEST.TEMP.STAGE_ZMM_C36
)

SELECT *
FROM L0_TEST