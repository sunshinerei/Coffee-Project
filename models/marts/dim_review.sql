with attributes as (

    select * from {{ ref('dim_attributes') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

final as (

    select
        dirty.coffee_id,
        attributes.review_date,
        attributes.rating,
        ROUND(attributes.USD_100g/(attributes.rating-50), 2) AS cost_per_point,
        dirty.aroma,
        dirty.acid,
        dirty.body,
        dirty.flavor,
        dirty.aftertaste,
        dirty.with_milk,
        attributes.desc_1 as blind_assessment,
        attributes_desc_2 as notes,
        attributes_desc_3 as bottom_line,
        STRING_TO_ARRAY(SPLIT_PART(clean.desc_1, '.', 2)) AS tasting_notes

    from attributes
    left join dirty using (coffee_id)

)

select * from final


