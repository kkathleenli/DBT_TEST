{% macro lstrip(value, chars=' ') %}
  {{ return(value | replace(chars, '') if value.startswith(chars) else value) }}
{% endmacro %}