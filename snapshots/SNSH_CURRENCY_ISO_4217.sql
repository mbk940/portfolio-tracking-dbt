{% snapshot SNSH_CURRENCY_ISO_4217 %}

{{
    config(
      unique_key= 'CURRENCY_HKEY',
      strategy='check',
      check_cols=['CURRENCY_HDIFF'],
    )
}}

select * from {{ ref('STG_currency_ISO_4217') }}

{% endsnapshot %}