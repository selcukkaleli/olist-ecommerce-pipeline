with source as (
    select * from {{ ref('marts_seller_monthly_metrics') }}
),

final as (
    select
        seller_id,
        AVG(growth) as avg_growth

    from source
    
    GROUP BY 
       seller_id

    HAVING COUNT(order_month) >= 3

)

select * from final
