{{
  config(
    materialized='table'
  )
}}

{% set sap_tables = ['MSEG', 'MKPF'] %}

{% set column_map_query %}
{% for i in sap_tables %}
SELECT TABNAME, ltrim(FIELDNAME,'_') AS FIELDNAME, DDLANGUAGE, TRIM(REPLACE(FIELDTEXT,'"','')) AS FIELDTEXT
FROM TEST.TEMP.DDFTX_CLEANED
WHERE DDLANGUAGE = 'E' AND TABNAME = '{{ i }}'
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
{% endset %}


{% set results =  run_query(column_map_query) %}
{% if execute %}
{% set old_list = results.columns[1].values() %}
{% set new_list = results.columns[3].values() %}
{% else %}
{% set old_list = [] %}
{% set new_list = [] %}
{% endif %}

{{log(old_list, info=True)}}
{{log(new_list, info=True)}}

{% set source_columns =  adapter.get_columns_in_relation(ref('dbt_test_l0_mseg_mkpf')) %}


WITH FINAL AS (
SELECT
{% set used_new_list = [] %}
{% for i in range(source_columns | length) %}

{% if source_columns[i].name in old_list and new_col_name != '' %}
{% set idx_in_old = old_list.index(source_columns[i].name) %}
{% set new_col_name = new_list[idx_in_old] %}
{% set original_new_col_name = new_col_name %}

{% if new_col_name in used_new_list %}
"{{ source_columns[i].name }}" AS "{{original_new_col_name}}_{{source_columns[i].name}}" {% if not loop.last %},{% endif %}
{% set new_col_name_combimed = original_new_col_name~ '_' ~ source_columns[i].name %}
{% do used_new_list.append(new_col_name_combimed) %}

{% else %}
"{{ source_columns[i].name }}" AS "{{ new_col_name }}" {% if not loop.last %},{% endif %}
{% do used_new_list.append(new_col_name) %}

{% endif %}

{% else %}
"{{ source_columns[i].name }}" AS "{{ source_columns[i].name }}"{% if not loop.last %},{% endif %}

{% endif %}
{% endfor %}
FROM {{ ref('dbt_test_l0_mseg_mkpf') }}
)

SELECT *
FROM FINAL