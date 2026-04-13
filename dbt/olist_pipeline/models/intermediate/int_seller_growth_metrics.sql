with source as (
    select * from {{ ref('int_seller_monthly_metrics') }}
),

final as (
    select
        seller_id,
        total_item_sold,
        avg_freight_value,
        total_revenue,
        LAG (total_revenue, 1, 0) OVER (PARTITION BY seller_id ORDER BY order_month ASC) AS prev_month_tot_revenue,
        avg_approving_time,
        avg_delivery_delay,
        avg_review_score,
        order_month

    from source
    
)

select * from final
