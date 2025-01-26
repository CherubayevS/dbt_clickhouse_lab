{{
    config (
        engine='MergeTree()',
        order_by=['O_ORDERYEAR']
    )
}}

SELECT
    toYear(O_ORDERDATE) AS O_ORDERYEAR,
    O_ORDERSTATUS,
    O_ORDERPRIORITY,
    count(DISTINCT O_ORDERKEY) AS num_orders,
    count(DISTINCT C_CUSTKEY) AS num_customers,
    SUM(L.L_EXTENDEDPRICE * L.L_DISCOUNT) AS revenue
FROM
    {{ ref('stg_orders') }} O
    INNER JOIN {{ ref('stg_customer') }} C ON O.O_CUSTKEY = C.C_CUSTKEY
    INNER JOIN {{ ref('stg_lineitem') }} L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE
    1 = 1
GROUP BY
    toYear(O_ORDERDATE),
    O_ORDERSTATUS,
    O_ORDERPRIORITY