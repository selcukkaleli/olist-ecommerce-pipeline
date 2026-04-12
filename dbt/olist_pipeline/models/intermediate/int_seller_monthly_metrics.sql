with source as (
    select * from {{ ref('stg_orders_with_details') }}
),

agg as (
    select
        seller_id,
        COUNT(order_id) as total_item_sold,
        AVG(freight_value) as avg_freight_value,
        SUM(price) as total_revenue,
        AVG(TIMESTAMP_DIFF(order_approved_at, order_purchase_timestamp, DAY)) as avg_approving_time,
        AVG(TIMESTAMP_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY)) as avg_delivery_delay,
        AVG(review_score) as avg_review_score,
        DATE_TRUNC(order_purchase_timestamp, MONTH) as order_month

    from source

    group by
        seller_id, order_month
    
)

select * from agg
