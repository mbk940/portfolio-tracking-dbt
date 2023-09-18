WITH
    current_from_snapshot as (
    {{ current_from_snapshot( snsh_ref = ref('SNSH_CURRENCY_ISO_4217') ) }}
    )
SELECT *
FROM current_from_snapshot