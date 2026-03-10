with stg_orders as (
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from {{ source('northwind', 'Orders') }}
),

stg_order_details as (
    select
        orderid,
        productid,
        quantity,
        quantity * unitprice as extendedpriceamount,
        quantity * unitprice * discount as discountamount,
        (quantity * unitprice) - (quantity * unitprice * discount) as soldamount
    from {{ source('northwind', 'Order_Details') }}
)

select
    o.orderid,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    {{ dbt_utils.generate_surrogate_key(['od.productid']) }} as productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_orders o
    join stg_order_details od on o.orderid = od.orderid