with clean as (

    select * from {{ ref('stg_clean') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

final as (

    select
        clean.coffee_id,
        clean.name,
        clean.roaster,
        clean.roast,
        clean.loc_country,
        clean.origin_1,
        clean.origin_2,
        clean.USD_100g,
        dirty.agtron

    from clean
    
    left join dirty using (coffee_id)
    WHERE clean.roast IS NOT NULL

)

select * from final