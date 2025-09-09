-- Orders fact table
-- This model creates a comprehensive orders fact table with customer and order details

with orders_staging as (
    select * from {{ ref('stg_orders') }}
),

customers_dim as (
    select * from {{ ref('dim_customers') }}
),

orders_fact as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.amount as original_amount,
        o.cleaned_amount,
        o.standardized_status,
        o.processed_at,
        -- Customer dimension attributes
        c.first_name,
        c.last_name,
        c.email,
        c.customer_segment,
        c.total_orders as customer_total_orders,
        c.total_spent as customer_total_spent,
        c.avg_order_value as customer_avg_order_value,
        -- Calculated fields
        case 
            when o.cleaned_amount > c.avg_order_value * 1.5 then 'high_value'
            when o.cleaned_amount < c.avg_order_value * 0.5 then 'low_value'
            else 'normal_value'
        end as order_value_category,
        -- Date dimensions
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month,
        extract(day from o.order_date) as order_day,
        extract(dow from o.order_date) as order_day_of_week,
        case 
            when extract(dow from o.order_date) in (0, 6) then 'weekend'
            else 'weekday'
        end as order_day_type,
        current_timestamp as fact_created_at
    from orders_staging o
    left join customers_dim c on o.customer_id = c.customer_id
)

select * from orders_fact
