{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- set csn = (custom_schema_name | trim) if custom_schema_name is not none else None -%}

  {%- if csn is none or csn == '' -%}
    {{ default_schema }}
  {%- elif csn | lower == 'appstoreconnect' -%}
    AppStoreConnect
  {%- elif csn | lower == 'googleplay' -%}
    GooglePlay
  {%- else -%}
    {{ default_schema }}_{{ csn }}
  {%- endif -%}
{%- endmacro %}


