with reviews as (

    select * from {{ ref('dim_review') }}

),

final as (

    select 
        coffee_id,
        tasting_notes
    from reviews,
         UNNEST(SPLIT(LOWER(tasting_notes))) as tasting_notes
         with OFFSET AS offset
         order by offset

)

select
    coffee_id,
    LTRIM(tasting_notes) as tasting_notes
from final
ORDER BY coffee_id