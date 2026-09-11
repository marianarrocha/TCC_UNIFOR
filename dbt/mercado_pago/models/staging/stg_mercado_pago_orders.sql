with source as (

    select *
    from read_json_auto(
        'data/bronze/mercado_pago/*.json'
    )

),

renamed as (

    select
        id as order_id,

        type as order_type,

        processing_mode,

        external_reference,

        cast(total_amount as decimal(18,2)) as total_amount,

        cast(total_paid_amount as decimal(18,2)) as total_paid_amount,

        country_code,

        user_id,

        status as order_status,

        status_detail as order_status_detail,

        capture_mode,

        currency,

        cast(created_date as timestamp) as created_at,

        cast(last_updated_date as timestamp) as updated_at,

        transactions

    from source

)

select *
from renamed