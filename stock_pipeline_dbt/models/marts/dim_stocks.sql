with source as (

    select * from {{ ref('stg_stock_prices') }}

),

dim_stocks as (

    select distinct
        ticker                              as symbol,
        case
            when ticker = 'AAPL' then 'Apple Inc.'
            when ticker = 'GOOGL' then 'Alphabet Inc.'
            when ticker = 'MSFT' then 'Microsoft Corporation'
            when ticker = 'TSLA' then 'Tesla Inc.'
            when ticker = 'AMZN' then 'Amazon.com Inc.'
        end                                 as company_name,
        case
            when ticker = 'TSLA' then 'Automotive'
            when ticker = 'AMZN' then 'Consumer Cyclical'
            else 'Technology'
        end                                 as sector,
        'NASDAQ'                            as exchange

    from source

)

select * from dim_stocks