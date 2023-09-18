{% snapshot SNSH_COUNTRY_ISO_4217 %}

{{
    config(
      unique_key= 'COUNTRY_HKEY',
      strategy='check',
      check_cols=['COUNTRY_HDIFF'],
    )
}}

select * from {{ ref('STG_country_ISO_3166') }}

{% endsnapshot %}