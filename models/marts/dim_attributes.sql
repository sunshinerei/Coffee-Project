with clean as (

    select * from {{ ref('stg_clean') }}

),

dirty as (

    select * from {{ ref('stg_dirty') }}

),

type as (

    select
        coffee_id,
        CASE
             WHEN CONTAINS_SUBSTR(desc_2 , 'hibrido de timor') THEN 'Timor Hybrid'
             WHEN CONTAINS_SUBSTR(desc_2 , 'timor hybrid') THEN 'Timor Hybrid'
             WHEN CONTAINS_SUBSTR(desc_2 , 'eugenioides') THEN 'Eugenioides'
             WHEN CONTAINS_SUBSTR(desc_2 , 'excelsa') THEN 'Excelsa'
             WHEN CONTAINS_SUBSTR(desc_2 , 'liberica') THEN 'Liberica'
             WHEN CONTAINS_SUBSTR(desc_2 , 'arabica') THEN 'Arabica'
             WHEN CONTAINS_SUBSTR(desc_2 , 'canephora') THEN 'Robusta'
             WHEN CONTAINS_SUBSTR(desc_2 , 'robusta') THEN 'Robusta'
             
             ELSE 'Unknown' END AS species,
        CASE 
             WHEN CONTAINS_SUBSTR(desc_2 , 'kona') THEN 'Kona'
             WHEN CONTAINS_SUBSTR(desc_2 , 'blue mountain') THEN 'Blue Mountain'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sumatra') THEN 'Sumatra'
             WHEN CONTAINS_SUBSTR(desc_2 , 'criolla') THEN 'Criolla'
             WHEN CONTAINS_SUBSTR(desc_2 , 'arabigo') THEN 'Arabigo'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pluma hidalgo') THEN 'Pluma Hidalgo'
             WHEN CONTAINS_SUBSTR(desc_2 , 'bergundal') THEN 'Bergundal AKA Garundang'
             WHEN CONTAINS_SUBSTR(desc_2 , 'garundang') THEN 'Bergundal AKA Garundang'
             WHEN CONTAINS_SUBSTR(desc_2 , 'san bernardo') THEN 'San Bernardo AKA Pache'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pache') THEN 'San Bernardo AKA Pache'
             WHEN CONTAINS_SUBSTR(desc_2 , 'san ramon') THEN 'San Ramon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'hickumalgur') THEN 'Chickumalgur'
             WHEN CONTAINS_SUBSTR(desc_2 , 'blawan paumah') THEN 'Blawan Paumah'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sidikalang') THEN 'Sidikalang'
             WHEN CONTAINS_SUBSTR(desc_2 , 'k7') THEN 'K7'
             WHEN CONTAINS_SUBSTR(desc_2 , 'k20') THEN 'K20'
             WHEN CONTAINS_SUBSTR(desc_2 , 'bmj') THEN 'BMJ'
             WHEN CONTAINS_SUBSTR(desc_2 , 'guatemala') THEN 'Guatemala'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pache comum') THEN 'Pache Comum'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pache colis') THEN 'Pache Colis'
             WHEN CONTAINS_SUBSTR(desc_2 , 'villalobos') THEN 'Villalobos'
             WHEN CONTAINS_SUBSTR(desc_2 , 'amarello de botucatu') THEN 'Amarello de Botucatu'

             WHEN CONTAINS_SUBSTR(desc_2 , 'french mission') THEN 'French Mission'
             WHEN CONTAINS_SUBSTR(desc_2 , 'yellow bourbon') THEN 'Yellow Bourbon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'red bourbon') THEN 'Red Bourbon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'orange bourbon') THEN 'Orange Bourbon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pink bourbon') THEN 'Pink Bourbon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mibrizi') THEN 'Mibrizi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mayaguez') THEN 'Mayaguez'
             WHEN CONTAINS_SUBSTR(desc_2 , 'bourbon chocola') THEN 'Bourbon Chocola'
             WHEN CONTAINS_SUBSTR(desc_2 , 'semperflorens') THEN 'Semperflorens'
             WHEN CONTAINS_SUBSTR(desc_2 , 'arusha') THEN 'Arusha'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ibairi') THEN 'Ibairi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'cera') THEN 'Cera'
             WHEN CONTAINS_SUBSTR(desc_2 , 'jackson 2/1257') THEN 'Jackson 2/1257'
             WHEN CONTAINS_SUBSTR(desc_2 , 'jackson') THEN 'Jackson'
             
             WHEN CONTAINS_SUBSTR(desc_2 , 'bourbon') THEN 'Bourbon'
             WHEN CONTAINS_SUBSTR(desc_2 , 'typica') THEN 'Typica'

             WHEN CONTAINS_SUBSTR(desc_2 , 'gesha') THEN 'Gesha'
             WHEN CONTAINS_SUBSTR(desc_2 , 'harrar') THEN 'Harrar'
             WHEN CONTAINS_SUBSTR(desc_2 , 'yirgacheffe') THEN 'Yirgacheffe'
             WHEN CONTAINS_SUBSTR(desc_2 , 'djimma') THEN 'Djimma'
             WHEN CONTAINS_SUBSTR(desc_2 , 'lekempti') THEN 'Lekempti'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sidamo') THEN 'Sidamo'
             WHEN CONTAINS_SUBSTR(desc_2 , 'agaro') THEN 'Agaro'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mugi') THEN 'Mugi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'wellega') THEN 'Wellega'
             WHEN CONTAINS_SUBSTR(desc_2 , 'melka') THEN 'Melka'
             WHEN CONTAINS_SUBSTR(desc_2 , 'longberry harrar') THEN 'Longberry Harrar'
             WHEN CONTAINS_SUBSTR(desc_2 , 'haru') THEN 'Haru'
             WHEN CONTAINS_SUBSTR(desc_2 , 'gera') THEN 'Gera'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mettu') THEN 'Mettu'
             WHEN CONTAINS_SUBSTR(desc_2 , 'awada') THEN 'Awada'
             WHEN CONTAINS_SUBSTR(desc_2 , 'wenago') THEN 'Wenago'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mechara') THEN 'Mechara'
             WHEN CONTAINS_SUBSTR(desc_2 , 'alghe') THEN 'Alghe'
             WHEN CONTAINS_SUBSTR(desc_2 , 'cioccie s6') THEN 'Cioccie S6'
             WHEN CONTAINS_SUBSTR(desc_2 , 'dalle') THEN 'Dalle'
             WHEN CONTAINS_SUBSTR(desc_2 , 'abyssinia') THEN 'Abyssinia'
             WHEN CONTAINS_SUBSTR(desc_2 , 'gawe') THEN 'Gawe'
             WHEN CONTAINS_SUBSTR(desc_2 , 'melko-ch2') THEN 'Melko-CH2'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ababuna') THEN 'Ababuna'
             WHEN CONTAINS_SUBSTR(desc_2 , 'tegu') THEN 'Tegu'
             WHEN CONTAINS_SUBSTR(desc_2 , 'rambung') THEN 'Rambung'
             WHEN CONTAINS_SUBSTR(desc_2 , 'tafarikela') THEN 'Tafarikela'
             WHEN CONTAINS_SUBSTR(desc_2 , 'dega') THEN 'Dega'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ennarea') THEN 'Ennarea'
             WHEN CONTAINS_SUBSTR(desc_2 , 'dilla') THEN 'Dilla'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ghimbi') THEN 'Ghimbi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'usda 762') THEN 'USDA 762'
             WHEN CONTAINS_SUBSTR(desc_2 , 'barbuk sudan') THEN 'Barbuk Sudan'
             WHEN CONTAINS_SUBSTR(desc_2 , 'rume sudan') THEN 'Rume Sudan'

             WHEN CONTAINS_SUBSTR(desc_2 , 'java') THEN 'Java'
             WHEN CONTAINS_SUBSTR(desc_2 , 'maragogype') THEN 'Maragogype'
             WHEN CONTAINS_SUBSTR(desc_2 , 'kent') THEN 'Kent'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pointu') THEN 'Pointu AKA Laurina'
             WHEN CONTAINS_SUBSTR(desc_2 , 'laurina') THEN 'Pointu AKA Laurina'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sl28') THEN 'SL28'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sl34') THEN 'SL34'
             WHEN CONTAINS_SUBSTR(desc_2 , 'tekisic') THEN 'Tekisic'
             WHEN CONTAINS_SUBSTR(desc_2 , 'villa sarchi') THEN 'Villa Sarchi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pacas') THEN 'Pacas'
             WHEN CONTAINS_SUBSTR(desc_2 , 'pacamara') THEN 'Pacamara'
             WHEN CONTAINS_SUBSTR(desc_2 , 'caturra') THEN 'Caturra'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mocha') THEN 'Mocha AKA Mokka'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mokka') THEN 'Mocha AKA Mokka'
             WHEN CONTAINS_SUBSTR(desc_2 , 'batian') THEN 'Battian'

             WHEN CONTAINS_SUBSTR(desc_2 , 'acaia') THEN 'Acaia'
             WHEN CONTAINS_SUBSTR(desc_2 , 'mundo novo') THEN 'Mundo Novo'
             WHEN CONTAINS_SUBSTR(desc_2 , 'catuai') THEN 'Catuai'
             WHEN CONTAINS_SUBSTR(desc_2 , 'maracaturra') THEN 'Maracaturra'
             WHEN CONTAINS_SUBSTR(desc_2 , 'rubi') THEN 'Rubi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ouro verde') THEN 'Ouro Verde'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ouro bronze') THEN 'Ouro Bronze'

             WHEN CONTAINS_SUBSTR(desc_2 , 'catimor') THEN 'Catimor'
             WHEN CONTAINS_SUBSTR(desc_2 , 'colombia') THEN 'Colombia'
             WHEN CONTAINS_SUBSTR(desc_2 , 'icatu') THEN 'Icatu'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ihcafe 90') THEN 'IHCAFE 90'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ruiru 11') THEN 'Ruiru 11'
             WHEN CONTAINS_SUBSTR(desc_2 , 'anacafe 14') THEN 'Anacafe 14'
             WHEN CONTAINS_SUBSTR(desc_2 , 'sarchimor') THEN 'Sarchimor'
             WHEN CONTAINS_SUBSTR(desc_2 , 'castillo') THEN 'Castillo'
             WHEN CONTAINS_SUBSTR(desc_2 , 'oro azteca') THEN 'Oro Azteca'
             WHEN CONTAINS_SUBSTR(desc_2 , 'tupi') THEN 'Tupi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'obata') THEN 'Obata'
             WHEN CONTAINS_SUBSTR(desc_2 , 'catrenic') THEN 'Catrenic'
             WHEN CONTAINS_SUBSTR(desc_2 , 'paraiso') THEN 'Paraiso'
             WHEN CONTAINS_SUBSTR(desc_2 , 'rasuna') THEN 'Rasuna'
             WHEN CONTAINS_SUBSTR(desc_2 , 'catucai') THEN 'Catucai'
             WHEN CONTAINS_SUBSTR(desc_2 , 'lempira') THEN 'Lempira'
             WHEN CONTAINS_SUBSTR(desc_2 , 'maracatu') THEN 'Maracatu'
             WHEN CONTAINS_SUBSTR(desc_2 , 'catiga mg2') THEN 'Catiga MG2'
             WHEN CONTAINS_SUBSTR(desc_2 , 'selection 9') THEN 'Selection 9'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ateng') THEN 'Ateng'
             WHEN CONTAINS_SUBSTR(desc_2 , 'parainema') THEN 'Parainema'
             WHEN CONTAINS_SUBSTR(desc_2 , 'arla') THEN 'Arla'
             WHEN CONTAINS_SUBSTR(desc_2 , 'icafe 95') THEN 'ICAFE 95'
             WHEN CONTAINS_SUBSTR(desc_2 , 'iapar 59') THEN 'IAPAR 59'
             WHEN CONTAINS_SUBSTR(desc_2 , 'ipar 103') THEN 'IPAR 103'
             WHEN CONTAINS_SUBSTR(desc_2 , 'tabi') THEN 'Tabi'
             WHEN CONTAINS_SUBSTR(desc_2 , 'costa rica 95') THEN 'Costa Rica 95 AKA CR-95'
             WHEN CONTAINS_SUBSTR(desc_2 , 'cr-95') THEN 'Costa Rica 95 AKA CR-95'
             WHEN CONTAINS_SUBSTR(desc_2 , 'devamachy') THEN 'Devamachy'
             WHEN CONTAINS_SUBSTR(desc_2 , 'marsellesa') THEN 'Marsellesa'
             WHEN CONTAINS_SUBSTR(desc_2 , 'catigua') THEN 'Catigua'
             WHEN CONTAINS_SUBSTR(desc_2 , 'bogor prada') THEN 'Bogor Prada'

             WHEN CONTAINS_SUBSTR(desc_2 , 'jember') THEN 'Jember AKA S795'
             WHEN CONTAINS_SUBSTR(desc_2 , 's795') THEN 'Jember AKA S795'
             WHEN CONTAINS_SUBSTR(desc_2 , 's288') THEN 'S288'
             WHEN CONTAINS_SUBSTR(desc_2 , 's26') THEN 'S26'
             WHEN CONTAINS_SUBSTR(desc_2 , 'kalimas') THEN 'Kalimas'
             WHEN CONTAINS_SUBSTR(desc_2 , 'menado') THEN 'Menado'
             WHEN CONTAINS_SUBSTR(desc_2 , 'kawisari') THEN 'Kawisari'

             ELSE 'Unknown' END AS variety_cultivar

    FROM clean

),

processing as (

    select
        coffee_id,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'natural') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'natural') THEN 'Yes'

        ELSE 'No' END AS natural_process,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'washed') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'washed') THEN 'Yes'

        ELSE 'No' END AS washed_process,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'wet_hulled') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'wet_hulled') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(name , 'semi-washed') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'semi-washed') THEN 'Yes'

        ELSE 'No' END AS semi_washed_process,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'honey') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'honey') THEN 'Yes'

        ELSE 'No' END AS honey_process,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'fruit maceration') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'fruit maceration') THEN 'Yes'

        ELSE 'No' END AS fruit_maceration,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'anaerobic') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'anaerobic') THEN 'Yes'

        ELSE 'No' END AS anaerobic_fermentation,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'lactic') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'lactic') THEN 'Yes'

        ELSE 'No' END AS lactic_fermentation,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'carbonic maceration') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'carbonic maceration') THEN 'Yes'

        ELSE 'No' END AS carbonic_fermentation,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'wine yeast') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'wine yeast') THEN 'Yes'

        ELSE 'No' END AS wine_fermentation,
        CASE
             WHEN CONTAINS_SUBSTR(name , 'koji') THEN 'Yes'
             WHEN CONTAINS_SUBSTR(desc_2 , 'koji') THEN 'Yes'

        ELSE 'No' END AS koji_fermentation

    FROM clean

),

agtron as (

    select
        coffee_id,
        agtron,
        whole_bean_agtron,
        grounds_agtron,
        (SAFE_CAST(whole_bean_agtron AS INTEGER)+SAFE_CAST(grounds_agtron AS INTEGER))/2 AS avg_agtron
    
    from
    (
    select
        coffee_id,
        agtron,
        SPLIT(agtron, '/')[OFFSET(0)] AS whole_bean_agtron,
        SPLIT(agtron, '/')[OFFSET(1)] AS grounds_agtron
    from dirty) temp_1

),

roast as (

    select
        coffee_id,
        CASE
            WHEN avg_agtron >  80 THEN 'Lighter than Arabic Roast'
            WHEN avg_agtron <= 80 AND avg_agtron > 70 THEN 'Arabic Roast'
            WHEN avg_agtron <= 70 AND avg_agtron > 58 THEN 'Cinnammon Roast'
            WHEN avg_agtron <= 58 AND avg_agtron > 50 THEN 'New England Roast'
            WHEN avg_agtron <= 50 AND avg_agtron > 45 THEN 'American Roast'
            WHEN avg_agtron <= 45 AND avg_agtron > 40 THEN 'City Roast'
            WHEN avg_agtron <= 40 AND avg_agtron > 35 THEN 'Full City Roast'
            WHEN avg_agtron <= 35 AND avg_agtron > 30 THEN 'Vienna Roast'
            WHEN avg_agtron <= 30 AND avg_agtron > 25 THEN 'French Roast'
            WHEN avg_agtron <= 25 AND avg_agtron > 15 THEN 'Italian Roast'
            WHEN avg_agtron <= 15 THEN 'Darker than Italian Roast'

        END as roast_name

    from agtron

),

final as (

    select
        clean.coffee_id,
        clean.name,
        clean.roaster,
        clean.roast,
        roast.roast_name,
        agtron.avg_agtron,
        agtron.agtron,
        agtron.whole_bean_agtron,
        agtron.grounds_agtron,
        clean.loc_country as roaster_country,
        clean.origin_1,
        clean.origin_2,
        clean.USD_100g,
        type.species,
        type.variety_cultivar,
        processing.natural_process,
        processing.washed_process,
        processing.semi_washed_process,
        processing.honey_process,
        processing.fruit_maceration,
        processing.anaerobic_fermentation,
        processing.lactic_fermentation,
        processing.carbonic_fermentation,
        processing.wine_fermentation,
        processing.koji_fermentation

    from clean
    left join type ON clean.coffee_id = type.coffee_id
    left join processing ON clean.coffee_id = processing.coffee_id
    left join agtron ON clean.coffee_id = agtron.coffee_id
    left join roast ON clean.coffee_id = roast.coffee_id

)

select * from final