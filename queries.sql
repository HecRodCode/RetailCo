-- queries.sql — Analytical queries over the star schema


-- Query 1: total revenue and order count grouped by month
SELECT
    d.year,
    d.month,
    SUM(f.amount)   AS total_revenue,
    COUNT(*)        AS total_orders
FROM FACTS_SALES f
JOIN DIM_DATE d ON f.id_date = d.id_date
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- Query 2: SKUs whose last-month sales exceed their historical average
WITH historical_avg AS (
    -- average revenue per SKU across all time
    SELECT
        p.sku,
        AVG(f.amount) AS avg_revenue
    FROM FACTS_SALES f
    JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
    GROUP BY p.sku
),
last_month_sales AS (
    -- total revenue per SKU in the most recent month available
    SELECT
        p.sku,
        SUM(f.amount) AS last_month_revenue
    FROM FACTS_SALES f
    JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
    JOIN DIM_DATE d     ON f.id_date    = d.id_date
    WHERE (d.year, d.month) = (
        SELECT year, month
        FROM DIM_DATE
        ORDER BY full_date DESC
        LIMIT 1
    )
    GROUP BY p.sku
)
-- keep only SKUs that beat their own average last month
SELECT
    lm.sku,
    lm.last_month_revenue,
    ha.avg_revenue
FROM last_month_sales lm
JOIN historical_avg ha ON lm.sku = ha.sku
WHERE lm.last_month_revenue > ha.avg_revenue
ORDER BY lm.last_month_revenue DESC;


-- Query 3: view with total amount, qty and average ticket per category
CREATE OR REPLACE VIEW ventas_por_categoria AS
SELECT
    p.category,
    SUM(f.amount)        AS total_amount,
    SUM(f.quantity)      AS total_qty,
    ROUND(
        SUM(f.amount) / NULLIF(SUM(f.quantity), 0), 2
    )                    AS ticket_promedio
FROM FACTS_SALES f
JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
GROUP BY p.category;

-- read the view
SELECT * FROM ventas_por_categoria ORDER BY total_amount DESC;


-- Query 4: top 5 states by revenue with their share of the total
SELECT
    s.ship_state,
    SUM(f.amount)                                       AS revenue,
    ROUND(
        SUM(f.amount) * 100.0 /
        SUM(SUM(f.amount)) OVER (), 2
    )                                                   AS pct_of_total
FROM FACTS_SALES f
JOIN DIM_SHIPMENTS s ON f.id_shipment = s.id_shipment
GROUP BY s.ship_state
ORDER BY revenue DESC
LIMIT 5;