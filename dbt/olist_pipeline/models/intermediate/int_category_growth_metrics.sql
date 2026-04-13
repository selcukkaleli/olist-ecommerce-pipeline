with source as (
    select * from {{ ref('int_category_monthly_metrics') }}
),

final as (
    select
        product_category_name_english,
        total_item_sold,
        avg_freight_value,
        total_revenue,
        LAG (total_revenue, 1, 0) OVER (PARTITION BY product_category_name_english ORDER BY order_month ASC) AS prev_month_tot_revenue,
        avg_approving_time,
        avg_delivery_delay,
        avg_review_score,
        order_month

    from source
    
)

select * from final
