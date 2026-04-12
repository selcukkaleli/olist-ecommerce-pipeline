with source as (
    select * from {{ source('raw', 'orders_enriched') }}
),

renamed as (
    select
        order_id,
        customer_unique_id,
        seller_id,
        product_id,
        order_item_id,
        review_id,
        product_category_name_english,
        order_status,
        price,
        freight_value,
        payment_type  as dominant_payment_method,
        total_payment_value,
        order_purchase_timestamp,
        order_approved_at,
        shipping_limit_date,
        order_estimated_delivery_date,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        CAST(review_score AS INT64) as review_score,
        CAST(review_creation_date AS TIMESTAMP) as review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) as review_answer_timestamp,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        customer_zip_code_prefix,
        customer_city,
        customer_state

    from source
)

select * from renamed
