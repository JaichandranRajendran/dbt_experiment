{% macro get_active_table_objects() %}
  {% set current_models = [] %}
  {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed", "snapshot"]) %}
    {% do current_models.append(node.name) %}
  {% endfor %}

  {% set schema_tables_query %}
    SELECT table_name
    FROM {{ target.database }}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '{{ target.schema }}'
  {% endset %}

  {% set db_tables = run_query(schema_tables_query).columns[0].values() %}

  {% set active_tables = db_tables | select("upper") | intersect(current_models | map("upper") | list) %}

  {% do log("Active tables with dbt models:", info=True) %}
  {% for table in active_tables %}
    {% do log(table, info=True) %}
  {% endfor %}

  {{ return(active_tables) }}
{% endmacro %}
