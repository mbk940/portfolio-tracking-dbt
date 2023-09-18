{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
        Name            as EXCHANGE_NAME        -- TEXT
         , ID           as EXCHANGE_CODE        -- TEXT
         , Country      as EXCHANGE_COUNTRY     -- TEXT
         , City         as EXCHANGE_CITY        -- TEXT
         , Zone         as EXCHANGE_ZONE        -- TEXT
         , Delta        as DELTA                -- FLOAT
         , DST_period   as DST_PERIOD           -- TEXT
         , Open         as OPEN                 -- TEXT
         , Close        as CLOSE                -- TEXT
         , Lunch        as LUNCH                -- TEXT
         , Open_UTC     as OPEN_UTC             -- TEXT
         , Close_UTC    as CLOSE_UTC            -- TEXT
         , Lunch_UTC    as LUNCH_UTC            -- TEXT
         , LOAD_TS      as LOAD_TS              -- TIMESTAMP_NTZ

         , 'SEED.exchange' as RECORD_SOURCE

    FROM {{ source('seeds', 'exchange') }}
 ),

default_record as (
    SELECT
        'Missing'   as EXCHANGE_NAME
        , '-1' as EXCHANGE_CODE       
        , 'Missing' as EXCHANGE_COUNTRY  
        , 'Missing' as EXCHANGE_CITY     
        , 'Missing' as EXCHANGE_ZONE     
        , '-1' as DELTA             
        , 'Missing' as DST_PERIOD        
        , '-1' as OPEN              
        , '-1' as CLOSE             
        , '-1' as LUNCH             
        , '-1' as OPEN_UTC          
        , '-1' as CLOSE_UTC         
        , '-1' as LUNCH_UTC   
        , '2020-01-01' as LOAD_TS_UTC
        , 'Missing' as RECORD_SOURCE            
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        concat_ws('|', EXCHANGE_CODE) as EXCHANGE_HKEY
        , concat_ws('|', EXCHANGE_NAME, EXCHANGE_CODE, EXCHANGE_COUNTRY,
                         EXCHANGE_CITY, EXCHANGE_ZONE, DELTA, DST_PERIOD, OPEN, CLOSE, LUNCH, OPEN_UTC, CLOSE_UTC, LUNCH_UTC ) as EXCHANGE_HDIFF

        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)
SELECT * FROM hashed
