with source as (

    select * from {{ source('coffee', 'dirty') }}

),

staged as (

    select
        DISTINCT CONCAT(name, roaster, review_date) AS coffee_id,
        agtron,
        aroma,
        acid,
        body,
        flavor,
        aftertaste,
        with_milk
    from source

)

select * from staged
