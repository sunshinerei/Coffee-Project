with attributes as (

    select * from {{ ref('dim_attributes') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

final as (

    select
        attributes.coffee_id,
        attributes.review_date,
        attributes.rating,
        dirty.aroma,
        dirty.acid,
        dirty.body,
        dirty.flavor,
        dirty.aftertaste,
        dirty.with_milk,
        attributes.desc_1 as blind_assessment,
        attributes_desc_2 as notes,
        attributes_desc_3 as bottom_line,
        [flavor notes]

    from attributes
    left join dirty using (coffee_id)

)

select * from final


