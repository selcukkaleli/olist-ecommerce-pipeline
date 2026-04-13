with source as (
    select * from {{ ref('int_seller_monthly_metrics') }}
),

final as (
    select
        seller_id,
        total_item_sold,
        avg_freight_value,
        total_revenue,
        avg_approving_time,
        avg_delivery_delay,
        avg_review_score,
        CAST(order_month AS DATE) as order_month

    from source
    
)

select * from final
