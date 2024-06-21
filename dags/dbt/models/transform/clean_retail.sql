-- models/deduplicated_raw_online_retail_table.sql

with ranked_retail as (
    select
        *,
        row_number() over (
            partition by InvoiceNo, StockCode, InvoiceDate, CustomerID
            order by InvoiceNo, StockCode, InvoiceDate, CustomerID
        ) as row_num
    from {{ source('retail', 'raw_online_retail_table') }}
)

select
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country
from ranked_retail
where row_num = 1
