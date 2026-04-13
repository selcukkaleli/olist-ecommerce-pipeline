with source as (
    select * from {{ ref('marts_category_monthly_metrics') }}
),

final as (
    select
        product_category_name_english,
        AVG(growth) as avg_growth

    from source
    
    GROUP BY 
        product_category_name_english

)

select * from final
