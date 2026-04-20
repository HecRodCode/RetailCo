import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Columns dropped due to high null rate or zero analytical value
COLUMNS_TO_DROP = ['Unnamed: 22', 'fulfilled-by', 'promotion-ids', 'index']

# Columns required downstream after transformation
REQUIRED_COLUMNS = [
    'Order ID', 'Date', 'SKU', 'Style', 'Category', 'Size',
    'ship-service-level', 'ship-city', 'ship-state',
    'ship-postal-code', 'ship-country',
    'Qty', 'Amount', 'average_ticket'
]



# STAGE 1 — EXTRACT
def extraer(ruta: str) -> pd.DataFrame:
    """Load raw CSV from disk and report the initial record count."""
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"Data file not found: {ruta}")

    df = pd.read_csv(ruta, low_memory=False)
    logger.info(f"[EXTRACT] Records loaded from source: {len(df):,}")
    return df



# STAGE 2 — TRANSFORM
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning and feature-engineering steps informed by EDA."""

    initial_count = len(df)

    # Drop columns with very high null rates or no analytical value
    df = df.drop(columns=[c for c in COLUMNS_TO_DROP if c in df.columns])

    # Remove exact duplicate rows — confirmed 0 in EDA, kept as a safeguard
    df = df.drop_duplicates()

    # Drop rows where Amount is null (6% of dataset) — no revenue means no fact
    df = df.dropna(subset=['Amount'])

    # Exclude zero-amount rows — no monetary value to record
    df = df[df['Amount'] > 0]

    # Exclude zero-or-negative quantity rows — EDA min is 0, those are invalid
    df = df[df['Qty'] > 0]

    # Parse Date from string to datetime — EDA flagged the column type as str
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Date'])

    # Normalize string columns to prevent case/whitespace mismatches in dimensions
    str_cols = [
        'ship-service-level', 'Category', 'Size',
        'ship-city', 'ship-state', 'ship-country', 'SKU', 'Style'
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    # Derive average ticket as revenue per unit sold
    df['average_ticket'] = (df['Amount'] / df['Qty']).round(2)

    # Retain only the columns consumed by the load stage
    df = df[[c for c in REQUIRED_COLUMNS if c in df.columns]]

    removed = initial_count - len(df)
    logger.info(f"[TRANSFORM] Records after cleaning: {len(df):,}  (removed {removed:,})")
    return df


# STAGE 3 — LOAD  (dimension helpers + fact insert)
def _load_dim_products(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_PRODUCTS and enrich the DataFrame with id_product."""
    products = df[['SKU', 'Style', 'Category', 'Size']].drop_duplicates(subset=['SKU'])

    records = [
        (row['SKU'], row['Style'], row['Category'], row['Size'])
        for _, row in products.iterrows()
    ]

    # Insert new products; skip rows that conflict with the unique SKU constraint
    execute_values(cur, """
        INSERT INTO DIM_PRODUCTS (sku, style, category, size)
        VALUES %s
        ON CONFLICT (sku) DO NOTHING;
    """, records)

    # Retrieve surrogate keys for every SKU in this batch
    cur.execute(
        "SELECT id_product, sku FROM DIM_PRODUCTS WHERE sku = ANY(%s);",
        (list(products['SKU'].tolist()),)
    )
    sku_map = {sku: id_product for id_product, sku in cur.fetchall()}

    df = df.copy()
    df['id_product'] = df['SKU'].map(sku_map)
    logger.info(f"[LOAD] DIM_PRODUCTS — unique SKUs processed: {len(records):,}")
    return df


def _load_dim_date(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_DATE and enrich the DataFrame with id_date."""
    dates = df['Date'].dt.normalize().drop_duplicates()

    records = [
        (
            d.date(),
            d.day,
            d.month,
            d.quarter,
            d.year,
            int(d.strftime('%V'))  # ISO week number
        )
        for d in dates
    ]

    # Insert new dates; skip rows that conflict with the unique full_date constraint
    execute_values(cur, """
        INSERT INTO DIM_DATE (full_date, day, month, quarter, year, week_of_year)
        VALUES %s
        ON CONFLICT (full_date) DO NOTHING;
    """, records)

    # Retrieve surrogate keys for every date in this batch
    cur.execute(
        "SELECT id_date, full_date FROM DIM_DATE WHERE full_date = ANY(%s);",
        ([r[0] for r in records],)
    )
    date_map = {full_date: id_date for id_date, full_date in cur.fetchall()}

    df = df.copy()
    df['id_date'] = df['Date'].dt.normalize().dt.date.map(date_map)
    logger.info(f"[LOAD] DIM_DATE — unique dates processed: {len(records):,}")
    return df


def _load_dim_shipments(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_SHIPMENTS and enrich the DataFrame with id_shipment."""
    ship_cols = ['ship-service-level', 'ship-city', 'ship-state',
                 'ship-postal-code', 'ship-country']

    shipments = df[ship_cols].drop_duplicates()

    records = [
        (
            row['ship-service-level'], row['ship-city'], row['ship-state'],
            str(row['ship-postal-code']), row['ship-country']
        )
        for _, row in shipments.iterrows()
    ]

    # Insert new shipment combos; skip rows that conflict with the unique composite constraint
    execute_values(cur, """
        INSERT INTO DIM_SHIPMENTS
            (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
        VALUES %s
        ON CONFLICT (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
        DO NOTHING;
    """, records)

    # Retrieve surrogate keys for every shipment combination in this batch
    cur.execute("""
        SELECT id_shipment, ship_service_level, ship_city, ship_state,
               ship_postal_code, ship_country
        FROM DIM_SHIPMENTS
        WHERE (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
              IN %s;
    """, (tuple(records),))

    ship_map = {
        (sl, ci, st, pc, co): sid
        for sid, sl, ci, st, pc, co in cur.fetchall()
    }

    df = df.copy()
    df['id_shipment'] = df.apply(
        lambda r: ship_map.get((
            r['ship-service-level'], r['ship-city'], r['ship-state'],
            str(r['ship-postal-code']), r['ship-country']
        )),
        axis=1
    )
    logger.info(f"[LOAD] DIM_SHIPMENTS — unique shipment combos processed: {len(records):,}")
    return df


def cargar(df: pd.DataFrame, conn: psycopg2.extensions.connection) -> None:
    """Orchestrate dimension upserts then insert clean records into FACTS_SALES."""
    if conn is None:
        logger.error("[LOAD] No database connection available. Aborting.")
        return

    cur = conn.cursor()

    try:
        # Resolve surrogate keys for all three dimensions
        df = _load_dim_products(df, cur)
        df = _load_dim_date(df, cur)
        df = _load_dim_shipments(df, cur)

        # Drop rows where any surrogate key lookup failed to protect FK constraints
        before = len(df)
        df = df.dropna(subset=['id_product', 'id_date', 'id_shipment'])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"[LOAD] {dropped:,} rows dropped — surrogate key lookup failed.")

        # Build fact records using only surrogate keys and measures
        fact_records = [
            (
                int(row['id_product']),
                int(row['id_date']),
                int(row['id_shipment']),
                float(row['Amount']),
                int(row['Qty']),
                float(row['average_ticket'])
            )
            for _, row in df.iterrows()
        ]

        execute_values(cur, """
            INSERT INTO FACTS_SALES
                (id_product, id_date, id_shipment, amount, quantity, average_ticket)
            VALUES %s;
        """, fact_records, page_size=500)

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM FACTS_SALES;")
        total_in_db = cur.fetchone()[0]
        logger.info(f"[LOAD] Total records in FACTS_SALES after insert: {total_in_db:,}")

    except Exception as exc:
        logger.error(f"[LOAD] Insert failed — rolling back: {exc}")
        conn.rollback()
    finally:
        cur.close()


# ENTRY POINT
if __name__ == "__main__":
    PATH_DATA = 'data/Amazon Sale Report.csv'

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

        logger.info("--- PIPELINE START ---")
        raw_df   = extraer(PATH_DATA)
        clean_df = transformar(raw_df)
        cargar(clean_df, db_conn)
        logger.info("--- PIPELINE COMPLETE ---")

    except FileNotFoundError as exc:
        logger.error(f"[EXTRACT] {exc}")
    except psycopg2.OperationalError as exc:
        logger.error(f"[CONNECTION] Could not connect to the database: {exc}")
    except Exception as exc:
        logger.error(f"[PIPELINE] Unexpected failure: {exc}")
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("Database connection closed.")