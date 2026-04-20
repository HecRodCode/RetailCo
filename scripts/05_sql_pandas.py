import os
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- SQL QUERIES ---

# Q1: total revenue and order count per month
Q1_MONTHLY_SALES = """
    SELECT
        d.year,
        d.month,
        SUM(f.amount)   AS total_revenue,
        COUNT(*)        AS total_orders
    FROM FACTS_SALES f
    JOIN DIM_DATE d ON f.id_date = d.id_date
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
"""

# Q2: SKUs whose last-month revenue exceeds their historical average
Q2_SKU_ABOVE_AVERAGE = """
    WITH historical_avg AS (
        SELECT
            p.sku,
            AVG(f.amount) AS avg_revenue
        FROM FACTS_SALES f
        JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
        GROUP BY p.sku
    ),
    last_month_sales AS (
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
    SELECT
        lm.sku,
        lm.last_month_revenue,
        ha.avg_revenue
    FROM last_month_sales lm
    JOIN historical_avg ha ON lm.sku = ha.sku
    WHERE lm.last_month_revenue > ha.avg_revenue
    ORDER BY lm.last_month_revenue DESC;
"""

# Q3a: create or replace the view (no result set)
Q3_CREATE_VIEW = """
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
"""

# Q3b: read from the view just created
Q3_READ_VIEW = "SELECT * FROM ventas_por_categoria ORDER BY total_amount DESC;"

# Q4: top 5 states by revenue with percentage share using window function
Q4_TOP_STATES = """
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
"""

# HELPERS
def run_query(label: str, sql: str, conn) -> pd.DataFrame:
    """Execute a SELECT and return the result as a DataFrame."""
    logger.info(f"Running: {label}")
    df = pd.read_sql(sql, conn)
    logger.info(f"  -> {len(df)} rows returned")
    return df


def create_view(sql: str, conn) -> None:
    """Execute a DDL statement (CREATE VIEW) that returns no rows."""
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("View 'ventas_por_categoria' created or replaced.")


# ENTRY POINT
if __name__ == "__main__":
    db_conn = None
    try:
        db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            port=os.getenv('DB_PORT', 5432)
        )
        logger.info("Database connection established.")

        # --- Q1: monthly sales summary ---
        df_q1 = run_query("Q1 — Monthly sales", Q1_MONTHLY_SALES, db_conn)
        print("\n=== Q1: Total revenue and orders per month ===")
        print(df_q1.to_string(index=False))

        # --- Q2: SKUs beating their historical average last month ---
        df_q2 = run_query("Q2 — SKUs above average", Q2_SKU_ABOVE_AVERAGE, db_conn)
        print("\n=== Q2: SKUs whose last-month revenue exceeds their historical average ===")
        print(df_q2.to_string(index=False))

        # --- Q3: create view then read it ---
        create_view(Q3_CREATE_VIEW, db_conn)
        df_q3 = run_query("Q3 — Sales by category (view)", Q3_READ_VIEW, db_conn)
        print("\n=== Q3: ventas_por_categoria view ===")
        print(df_q3.to_string(index=False))

        # --- Q4: top 5 states by revenue with share ---
        df_q4 = run_query("Q4 — Top 5 states by revenue", Q4_TOP_STATES, db_conn)
        print("\n=== Q4: Top 5 states — revenue and % of total ===")
        print(df_q4.to_string(index=False))

    except psycopg2.OperationalError as exc:
        logger.error(f"[CONNECTION] Could not connect to the database: {exc}")
    except Exception as exc:
        logger.error(f"[SCRIPT] Unexpected failure: {exc}")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("Database connection closed.")