{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {# seeds go in a global `raw` schema #}
    {% if node.resource_type == 'seed' %}
        {{ custom_schema_name | trim }}

    {# non-specified schemas go to the default target schema #}
    {% elif custom_schema_name is none %}
        {{ default_schema }}

    {# shared schemas bypass target prefixing #}
    {% set shared_schemas = var('shared_schemas', []) | map('trim') | list %}
    {% elif (custom_schema_name | trim) in shared_schemas %}
        {{ custom_schema_name | trim }}

    {# specified custom schema names go to the schema name prepended with the default schema name in prod #}
    {% elif target.name == 'prod' %}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {# specified custom schemas go to the default target schema for non-prod targets #}
    {% else %}
        {{ default_schema }}
    {% endif %}

{% endmacro %}
