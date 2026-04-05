with source as (

    select * from {{ source('public', 'stock_prices') }}

),

staged as (

    select
        date::date                          as price_date,
        symbol                              as ticker,
        open::float                         as open_price,
        high::float                         as high_price,
        low::float                          as low_price,
        close::float                        as close_price,
        volume::bigint                      as volume,
        round((high - low)::numeric, 2)     as daily_range,
        round((close - open)::numeric, 2)   as daily_change

    from source

)

select * from staged