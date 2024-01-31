with source as (

    select * from {{ source('coffee', 'dirty') }}

),

staged as (

    select
        translate(name, "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ", "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyy") as name,
        translate(roaster, "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ", "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyy") as roaster,
        review_date,
        agtron,
        aroma,
        acid,
        body,
        flavor,
        aftertaste,
        with_milk
    from source

)

select
DISTINCT CONCAT(name, roaster, review_date) AS coffee_id,
*
from staged
