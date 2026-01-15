{% macro surrogate_key(cols) -%}
  {#-
    cols: list of column expressions (strings), e.g. ["customer_id", "email"]
    Returns an md5 hash over concatenated, string-cast values.
  -#}
  md5(
    {%- for c in cols -%}
      coalesce(cast({{ c }} as varchar), '')
      {%- if not loop.last -%} || '|' || {%- endif -%}
    {%- endfor -%}
  )
{%- endmacro %}
