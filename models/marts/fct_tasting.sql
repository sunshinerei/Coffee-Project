with reviews as (

    select * from {{ ref('dim_review') }}

),

split_temp as (

    select 
        coffee_id,
        tasting_notes
    from reviews,
         UNNEST(SPLIT(LOWER(tasting_notes))) as tasting_notes
         with OFFSET AS offset
         order by offset

),

category as (

    select
        coffee_id,
        tasting_notes,
        CASE
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pine') THEN 'hoppy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'hop') THEN 'hoppy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'eucalyptus') THEN 'hoppy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sage') THEN 'hoppy'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'tobacco') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'scorched') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'smoky') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'toasted') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'roast-toned') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'leather') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'balsa') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'acrid') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'ash') THEN 'roasted'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'burnt') THEN 'roasted'
             
             WHEN CONTAINS_SUBSTR(tasting_notes , 'chocolate') THEN 'cocoa'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'chocolaty') THEN 'cocoa'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cocoa') THEN 'cocoa'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cacao') THEN 'cocoa'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'floral') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'flower') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'gardenia') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'freesia') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'magnolia') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'honeysuckle') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'jasmine') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'wisteria') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'verbena') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lily') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'nasturtium') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lilac') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'carnation') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'narcissus') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'blossom') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'violet') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'rhododendron') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'hibiscus') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'rose') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tea') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'wisteria') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'orchid') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lantana') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'alyssum') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lavender') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'perfumed') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'hyacinth') THEN 'floral'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'azalea') THEN 'floral'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'rum') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'grappa') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'brandy') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cognac') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'whiskey') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'wine') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'fermented') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'overripe') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'alcohol') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'yogurt') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'milk') THEN 'alcohol/fermented'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cheese') THEN 'alcohol/fermented'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'tangy') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tamarind') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cherry') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tart') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pomegranate') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'apple') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'rhubarb') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'currant') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'grape') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pinot noir') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pinapple') THEN 'tangy/tart'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tomato') THEN 'tangy/tart'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'fruit-toned') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'fruit-forward') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pear') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'banana') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'juicy') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'berry') THEN 'fruit-other'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'juniper') THEN 'fruit-other'
             
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cedar') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'wood') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'oak') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'fir') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mesquite') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'elm') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sassafras') THEN 'woody'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'bay tree') THEN 'woody'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'petrichor') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'earth') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'must') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'musk') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mold') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'damp') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'phenolic') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'animalic') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'papery') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cardboard') THEN 'earthy'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mushroom') THEN 'earthy'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'lemon') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'grapefruit') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'zest') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'orange') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lime') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'bergamot') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tangerine') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sumac') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'nectarine') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pomelo') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'kumquat') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'yuzu') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mandarin') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sour') THEN 'citrus'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'acid') THEN 'citrus'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'guava') THEN 'tropical fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'papaya') THEN 'tropical fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'lychee') THEN 'tropical fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'passion') THEN 'tropical fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'starfruit') THEN 'tropical fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mango') THEN 'tropical fruit'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'melon') THEN 'melon'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cantaloupe') THEN 'melon'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'honeydew') THEN 'melon'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'prune') THEN 'dried fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'date') THEN 'dried fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'raisin') THEN 'dried fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'persimmon') THEN 'dried fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'dried stone fruit') THEN 'dried fruit'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'peach') THEN 'stone fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'apricot') THEN 'stone fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'plum') THEN 'stone fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'fig') THEN 'stone fruit'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pluot') THEN 'stone fruit'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'marjoram') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'loquat') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'bay leaf') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'thyme') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'ginseng') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'tarragon') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sarsaparilla') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'ginger') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'clove') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'spice') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pepper') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'frankincense') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'star anise') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cinnamon') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cardamom') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'myrrh') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'coriander') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'mustard') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'medicinal') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'oungent') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'anise') THEN 'herbal/spice'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'nutmeg') THEN 'herbal/spice'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'nut') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'almond') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pistachio') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cashew') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'pecan') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'frangipane') THEN 'nutty'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'marzipan') THEN 'nutty'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'sweet') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'sugar') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'marshmallow') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'maple syrup') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'agave') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'honey') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'vanilla') THEN 'sweet'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'vanillin') THEN 'sweet'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'nougat') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'caramel') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'halvah') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'toffee') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'molasses') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'brown sugar') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'butterscotch') THEN 'caramel'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'praline') THEN 'caramel'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'malt') THEN 'cereal'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'cereal') THEN 'cereal'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'grain') THEN 'cereal'

             WHEN CONTAINS_SUBSTR(tasting_notes , 'green') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'vegetative') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'olive oil') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'raw') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'under-ripe') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'peapod') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'fresh') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'hay') THEN 'green/vegetative'
             WHEN CONTAINS_SUBSTR(tasting_notes , 'bean') THEN 'green/vegetative'

             ELSE 'other' END AS tasting_category

    FROM split_temp

)

select
    coffee_id,
    REPLACE((
    REPLACE((
    REPLACE((
    REPLACE((
    REPLACE(
    LTRIM(tasting_notes), ' in aroma and cup', ''))
    , 'a hint of ', ''))
    , 'fresh-cut ', ''))
    , 'hint of ', ''))
    , '-like flowers', '')
    
    as tasting_notes,
    tasting_category
from category