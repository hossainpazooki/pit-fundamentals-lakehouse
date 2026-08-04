{% macro as_of_ts() %}
  (TIMESTAMP '{{ var("as_of_date") }} 23:59:59' AT TIME ZONE 'America/New_York')
{% endmacro %}
