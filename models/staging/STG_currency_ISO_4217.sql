{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
        AlphabeticCode     as CURRENCY_ALPHABETIC_CODE      -- TEXT
         , NumericCode     as CURRENCY_NUMERIC_CODE         -- NUMBER
         , DecimalDigits   as DECIMAL_DIGITS                -- NUMBER
         , CurrencyName    as CURRENCY_NAME                 -- TEXT
         , Locations       as CURRENCY_LOCATIONS            -- TEXT
         , LOAD_TS         as LOAD_TS                       -- TIMESTAMP_NTZ

         , 'SEED.currency_ISO_4217' as RECORD_SOURCE

    FROM {{ source('seeds', 'currency_ISO_4217') }}
 ),

default_record as (
    SELECT
        '-1'            as CURRENCY_ALPHABETIC_CODE
        , -1            as CURRENCY_NUMERIC_CODE
        , -1            as DECIMAL_DIGITS
        , 'Missing'     as CURRENCY_NAME
        , 'Missing'     as CURRENCY_LOCATIONS
        , '2020-01-01'  as LOAD_TS_UTC
        , 'Missing'     as RECORD_SOURCE
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        concat_ws('|', CURRENCY_ALPHABETIC_CODE) as CURRENCY_HKEY
        , concat_ws('|', CURRENCY_ALPHABETIC_CODE, CURRENCY_NUMERIC_CODE, DECIMAL_DIGITS,
                         CURRENCY_NAME, CURRENCY_LOCATIONS ) as CURRENCY_HDIFF

        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)
SELECT * FROM hashed
