with clean as (

    select * from {{ ref('stg_clean') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

final as (

    select
        clean.coffee_id,
        clean.review_date,
        ROUND(clean.USD_100g/(clean.rating-50), 2) AS dollar_cost_per_point,
        clean.rating,
        dirty.aroma,
        dirty.acid,
        dirty.body,
        dirty.flavor,
        dirty.aftertaste,
        dirty.with_milk,
        clean.desc_1 as blind_assessment,
        clean.desc_2 as notes,
        clean.desc_3 as bottom_line,
        SPLIT(clean.desc_1, '.')[OFFSET(1)] AS tasting_notes

    from clean
    left join dirty using (coffee_id)

)

select * from final


