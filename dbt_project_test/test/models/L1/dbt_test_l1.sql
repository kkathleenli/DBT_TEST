{{
  config(
    materialized='table'
  )
}}

{% set column_map_query %}
SELECT IOBJNM AS old_column_name, REPLACE(TXTSH,' ','_') AS new_column_name
FROM TEST.TEMP.TEST_OBJ_KH_20240418
WHERE LANGU = 'E' AND OBJVERS = 'A'
{% endset %}


{% set results =  run_query(column_map_query) %}
{% if execute %}
{% set old_list = results.columns[0].values() %}
{% set new_list = results.columns[1].values() %}
{% else %}
{% set old_list = [] %}
{% set new_list = [] %}
{% endif %}


{% set source_columns =  adapter.get_columns_in_relation(ref('dbt_test_l0')) %}

WITH FINAL AS (
SELECT
{% for i in range(source_columns | length) %}

{% if source_columns[i].name in old_list %}
{% set idx_in_old = old_list.index(source_columns[i].name) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}" {% if not loop.last %},{% endif %}

{% elif source_columns[i].name.startswith('2') %}
{% set trimmed_source_columns = lstrip(source_columns[i].name,'2') %}
{% if trimmed_source_columns in old_list %}
{% set idx_in_old = old_list.index(trimmed_source_columns) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}_key" {% if not loop.last %},{% endif %}
{% endif %}

{% elif source_columns[i].name.startswith('4') %}
{% set trimmed_source_columns = lstrip(source_columns[i].name,'4') %}
{% if trimmed_source_columns in old_list %}
{% set idx_in_old = old_list.index(trimmed_source_columns) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}_4" {% if not loop.last %},{% endif %}
{% endif %}

{% elif source_columns[i].name.startswith('1') %}
{% set trimmed_source_columns = lstrip(source_columns[i].name,'1') %}
{% if trimmed_source_columns in old_list %}
{% set idx_in_old = old_list.index(trimmed_source_columns) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}_1" {% if not loop.last %},{% endif %}
{% endif %}

{% elif source_columns[i].name.startswith('5') %}
{% set trimmed_source_columns = lstrip(source_columns[i].name,'5') %}
{% if trimmed_source_columns in old_list %}
{% set idx_in_old = old_list.index(trimmed_source_columns) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}_5" {% if not loop.last %},{% endif %}
{% endif %}

{% elif source_columns[i].name.startswith('8') %}
{% set trimmed_source_columns = lstrip(source_columns[i].name,'8') %}
{% if trimmed_source_columns in old_list %}
{% set idx_in_old = old_list.index(trimmed_source_columns) %}
"{{source_columns[i].name}}" AS "{{ new_list[idx_in_old] }}_" {% if not loop.last %},{% endif %}
{% endif %}

{% else %}
"{{source_columns[i].name}}" AS "{{source_columns[i].name}}" {% if not loop.last %},{% endif %}

{% endif %}
{% endfor %}
FROM {{ ref('dbt_test_l0') }}
)

SELECT *
FROM FINAL
