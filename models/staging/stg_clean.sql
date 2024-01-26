with source as (
    select *
    from {{ source('coffee', 'clean') }}
),

staged as (
    select
        DISTINCT CONCAT(name, roaster, review_date) AS coffee_id,
        name,
        roaster,
        roast,
        loc_country,
        origin_1,
        origin_2,
        _100g_USD AS USD_100g,
        rating,
        review_date,
        desc_1,
        desc_2,
        desc_3
    from source
)

select * from staged