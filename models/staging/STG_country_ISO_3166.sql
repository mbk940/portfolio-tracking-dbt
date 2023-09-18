{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
        country_name                    as COUNTRY_NAME             -- TEXT
         , country_code_2_letter        as COUNTRY_CODE_2_LETTER    -- TEXT
         , country_code_3_letter        as COUNTRY_CODE_3_LETTER    -- TEXT
         , country_code_numeric         as COUNTRY_CODE_NUM         -- NUMBER
         , iso_3166_2                   as COUNTRY_ISO_3166_2       -- TEXT
         , region                       as REGION                   -- TEXT
         , sub_region                   as SUB_REGION               -- TEXT
         , intermediate_region          as INTERMEDIATE_REGION      -- TEXT
         , region_code                  as REGION_CODE              -- NUMBER
         , sub_region_code              as SUB_REGION_CODE          -- NUMBER
         , intermediate_region_code     as INTERMEDIATE_REGION_CODE -- NUMBER
         , LOAD_TS                      as LOAD_TS                  -- TIMESTAMP_NTZ

         , 'SEED.country_ISO_3166' as RECORD_SOURCE

    FROM {{ source('seeds', 'country_ISO_3166') }}
 ),

default_record as (
    SELECT
        'Missing'                       as COUNTRY_NAME             
         , 'Missing'                    as COUNTRY_CODE_2_LETTER    
         , 'Missing'                    as COUNTRY_CODE_3_LETTER    
         , '-1'                         as COUNTRY_CODE_NUM         
         , 'Missing'                    as COUNTRY_ISO_3166_2       
         , 'Missing'                    as REGION                   
         , 'Missing'                    as SUB_REGION               
         , 'Missing'                    as INTERMEDIATE_REGION      
         , -1                           as REGION_CODE              
         , -1                           as SUB_REGION_CODE          
         , -1                           as INTERMEDIATE_REGION_CODE 
         , '2020-01-01'                 as LOAD_TS_UTC     
         , 'Missing'                    as RECORD_SOURCE              
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        concat_ws('|', COUNTRY_CODE_NUM) as COUNTRY_HKEY
        , concat_ws('|', COUNTRY_NAME, COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER,
                         COUNTRY_CODE_NUM, COUNTRY_ISO_3166_2, REGION, SUB_REGION, INTERMEDIATE_REGION, REGION_CODE, SUB_REGION_CODE, INTERMEDIATE_REGION_CODE ) as COUNTRY_HDIFF

        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)
SELECT * FROM hashed
