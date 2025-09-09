-- Staging model for orders data
-- This model cleans and standardizes the raw orders data

with source_data as (
    select * from {{ source('raw_data', 'orders') }}
),

cleaned_orders as (
    select
        order_id,
        customer_id,
        order_date,
        amount,
        status,
        -- Add data quality checks
        case 
            when amount < 0 then 0
            else amount
        end as cleaned_amount,
        -- Standardize status values
        case 
            when lower(status) = 'pending' then 'pending'
            when lower(status) = 'completed' then 'completed'
            when lower(status) = 'cancelled' then 'cancelled'
            when lower(status) = 'shipped' then 'shipped'
            else 'unknown'
        end as standardized_status,
        current_timestamp as processed_at
    from source_data
)

select * from cleaned_orders
