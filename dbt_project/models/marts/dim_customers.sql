-- Customer dimension table
-- This model creates a comprehensive customer dimension with enriched data

with customer_source as (
    select * from {{ source('raw_data', 'customers') }}
),

customer_metrics as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.created_at,
        -- Calculate customer metrics
        count(o.order_id) as total_orders,
        sum(o.cleaned_amount) as total_spent,
        avg(o.cleaned_amount) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        case 
            when count(o.order_id) = 0 then 'new'
            when count(o.order_id) between 1 and 3 then 'regular'
            when count(o.order_id) between 4 and 10 then 'frequent'
            else 'vip'
        end as customer_segment,
        current_timestamp as dim_created_at
    from customer_source c
    left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
    group by c.customer_id, c.first_name, c.last_name, c.email, c.created_at
)

select * from customer_metrics
