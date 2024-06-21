with ranked_country as (
    select
        *,
        row_number() over (
            partition by id -- unique identifier for the country table
            order by id -- order by the unique identifier, or any other column if available
        ) as row_num
    from {{ source('retail', 'country') }}
)

select
    id,
    iso,
    name,
    nicename,
    iso3,
    numcode,
    phonecode
from ranked_country
where row_num = 1