with stock_prices as (

    select * from {{ ref('stg_stock_prices') }}

),

fct_stock_prices as (

    select
        price_date,
        ticker          as symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_range,
        daily_change,
        case
            when daily_change > 0 then 'UP'
            when daily_change < 0 then 'DOWN'
            else 'FLAT'
        end             as price_direction

    from stock_prices

)

select * from fct_stock_prices