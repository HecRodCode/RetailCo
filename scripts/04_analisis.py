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


# QUERIES

# Top 10 SKUs ranked by total revenue
Q_TOP_SKU = """
    SELECT
        p.sku,
        SUM(f.amount) AS total_revenue
    FROM FACTS_SALES f
    JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
    GROUP BY p.sku
    ORDER BY total_revenue DESC
    LIMIT 10;
"""

# Total revenue grouped by year and month
Q_MONTHLY_SALES = """
    SELECT
        d.year,
        d.month,
        SUM(f.amount) AS total_revenue
    FROM FACTS_SALES f
    JOIN DIM_DATE d ON f.id_date = d.id_date
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
"""

# Average ticket per product category
Q_AVG_TICKET_CATEGORY = """
    SELECT
        p.category,
        ROUND(AVG(f.average_ticket), 2) AS avg_ticket
    FROM FACTS_SALES f
    JOIN DIM_PRODUCTS p ON f.id_product = p.id_product
    GROUP BY p.category
    ORDER BY avg_ticket DESC;
"""

# Total revenue per sales channel (ship_service_level)
Q_REVENUE_BY_CHANNEL = """
    SELECT
        s.ship_service_level,
        SUM(f.amount) AS total_revenue
    FROM FACTS_SALES f
    JOIN DIM_SHIPMENTS s ON f.id_shipment = s.id_shipment
    GROUP BY s.ship_service_level
    ORDER BY total_revenue DESC;
"""

# HELPERS
def run_query(label: str, sql: str, conn) -> pd.DataFrame:
    """Execute a SELECT and return the result as a DataFrame."""
    logger.info(f"Running: {label}")
    df = pd.read_sql(sql, conn)
    logger.info(f"  -> {len(df)} rows returned")
    return df


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

        # --- Top 10 SKUs by revenue ---
        df_sku = run_query("Top 10 SKUs by revenue", Q_TOP_SKU, db_conn)
        print("\n=== Top 10 SKU por revenue total ===")
        print(df_sku.to_string(index=False))

        # --- Monthly sales ---
        df_monthly = run_query("Monthly sales", Q_MONTHLY_SALES, db_conn)
        print("\n=== Ventas totales por mes ===")
        print(df_monthly.to_string(index=False))

        # --- Average ticket per category ---
        df_ticket = run_query("Avg ticket by category", Q_AVG_TICKET_CATEGORY, db_conn)
        print("\n=== Ticket promedio por categoría ===")
        print(df_ticket.to_string(index=False))

        # --- Revenue by sales channel ---
        df_channel = run_query("Revenue by sales channel", Q_REVENUE_BY_CHANNEL, db_conn)
        print("\n=== Revenue por canal de venta ===")
        print(df_channel.to_string(index=False))

    except psycopg2.OperationalError as exc:
        logger.error(f"[CONNECTION] Could not connect to the database: {exc}")
    except Exception as exc:
        logger.error(f"[SCRIPT] Unexpected failure: {exc}")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("Database connection closed.")