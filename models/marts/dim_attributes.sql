with clean as (

    select * from {{ ref('stg_clean') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

final as (

    select
        clean.*,
        dirty.aroma,
        dirty.acid,
        dirty.body,
        dirty.flavor,
        dirty.aftertaste,
        dirty.with_milk

    from clean
    
    left join dirty using (coffee_id)

)

select * from final