with source as (

    select * from {{ source('coffee', 'dirty') }}

),

staged as (

    select
        CONCAT(name, roaster) AS coffee_id,
        aroma,
        acid,
        body,
        flavor,
        aftertaste,
        with_milk
    from source

)

select * from staged
