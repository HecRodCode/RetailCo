"""
ETL task functions — imported by the DAG.
Each function is a pure Python step; Airflow wraps them with @task.
"""
import os
import logging
import tempfile
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Columns removed due to high null rate or zero analytical value
_DROP_COLS = ['Unnamed: 22', 'fulfilled-by', 'promotion-ids', 'index']

# Columns kept after transformation for downstream loading
_KEEP_COLS = [
    'Order ID', 'Date', 'SKU', 'Style', 'Category', 'Size',
    'ship-service-level', 'ship-city', 'ship-state',
    'ship-postal-code', 'ship-country',
    'Qty', 'Amount', 'average_ticket'
]

# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------

def run_extract(csv_path: str) -> str:
    """Read the CSV and persist it as parquet; return the temp file path."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"[EXTRACT] Rows loaded: {len(df):,}")

    # Write to a shared temp location so the next task can read it
    tmp = tempfile.NamedTemporaryFile(
        suffix='.parquet', delete=False, dir='/tmp', prefix='etl_raw_'
    )
    df.to_parquet(tmp.name, index=False)
    logger.info(f"[EXTRACT] Raw data saved to {tmp.name}")
    return tmp.name


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------

def run_transform(raw_path: str) -> str:
    """Apply all cleaning steps; persist the clean DataFrame and return its path."""
    df = pd.read_parquet(raw_path)
    initial = len(df)

    # Remove low-value or high-null columns
    df = df.drop(columns=[c for c in _DROP_COLS if c in df.columns])

    # Safeguard deduplication
    df = df.drop_duplicates()

    # Drop rows with no monetary value
    df = df.dropna(subset=['Amount'])
    df = df[df['Amount'] > 0]

    # Drop rows with invalid quantity
    df = df[df['Qty'] > 0]

    # Parse date column from string to datetime
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Date'])

    # Normalize strings to avoid dimension mismatches
    _str_cols = [
        'ship-service-level', 'Category', 'Size',
        'ship-city', 'ship-state', 'ship-country', 'SKU', 'Style'
    ]
    for col in _str_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    # Derive average revenue per unit
    df['average_ticket'] = (df['Amount'] / df['Qty']).round(2)

    # Keep only what the load stage needs
    df = df[[c for c in _KEEP_COLS if c in df.columns]]

    logger.info(f"[TRANSFORM] Rows after cleaning: {len(df):,}  (removed {initial - len(df):,})")

    tmp = tempfile.NamedTemporaryFile(
        suffix='.parquet', delete=False, dir='/tmp', prefix='etl_clean_'
    )
    df.to_parquet(tmp.name, index=False)
    logger.info(f"[TRANSFORM] Clean data saved to {tmp.name}")
    return tmp.name


# ---------------------------------------------------------------------------
# LOAD — dimension helpers
# ---------------------------------------------------------------------------

def _upsert_dim_products(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_PRODUCTS and attach id_product to every row."""
    products = df[['SKU', 'Style', 'Category', 'Size']].drop_duplicates(subset=['SKU'])
    records = [(r.SKU, r.Style, r.Category, r.Size) for r in products.itertuples(index=False)]

    execute_values(cur, """
        INSERT INTO DIM_PRODUCTS (sku, style, category, size)
        VALUES %s
        ON CONFLICT (sku) DO NOTHING;
    """, records)

    cur.execute(
        "SELECT id_product, sku FROM DIM_PRODUCTS WHERE sku = ANY(%s);",
        (list(products['SKU']),)
    )
    sku_map = {sku: pid for pid, sku in cur.fetchall()}
    df = df.copy()
    df['id_product'] = df['SKU'].map(sku_map)
    logger.info(f"[LOAD] DIM_PRODUCTS — {len(records):,} SKUs processed")
    return df


def _upsert_dim_date(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_DATE and attach id_date to every row."""
    dates = df['Date'].dt.normalize().drop_duplicates()
    records = [
        (d.date(), d.day, d.month, d.quarter, d.year, int(d.strftime('%V')))
        for d in dates
    ]

    execute_values(cur, """
        INSERT INTO DIM_DATE (full_date, day, month, quarter, year, week_of_year)
        VALUES %s
        ON CONFLICT (full_date) DO NOTHING;
    """, records)

    cur.execute(
        "SELECT id_date, full_date FROM DIM_DATE WHERE full_date = ANY(%s);",
        ([r[0] for r in records],)
    )
    date_map = {fd: did for did, fd in cur.fetchall()}
    df = df.copy()
    df['id_date'] = df['Date'].dt.normalize().dt.date.map(date_map)
    logger.info(f"[LOAD] DIM_DATE — {len(records):,} dates processed")
    return df


def _upsert_dim_shipments(df: pd.DataFrame, cur) -> pd.DataFrame:
    """Upsert DIM_SHIPMENTS and attach id_shipment to every row."""
    ship_cols = ['ship-service-level', 'ship-city', 'ship-state',
                 'ship-postal-code', 'ship-country']
    shipments = df[ship_cols].drop_duplicates()
    records = [
        (r[0], r[1], r[2], str(r[3]), r[4])
        for r in shipments.itertuples(index=False)
    ]

    execute_values(cur, """
        INSERT INTO DIM_SHIPMENTS
            (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
        VALUES %s
        ON CONFLICT (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
        DO NOTHING;
    """, records)

    cur.execute("""
        SELECT id_shipment, ship_service_level, ship_city, ship_state,
               ship_postal_code, ship_country
        FROM DIM_SHIPMENTS
        WHERE (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country)
              IN %s;
    """, (tuple(records),))
    ship_map = {(sl, ci, st, pc, co): sid for sid, sl, ci, st, pc, co in cur.fetchall()}

    df = df.copy()
    df['id_shipment'] = df.apply(
        lambda r: ship_map.get((
            r['ship-service-level'], r['ship-city'], r['ship-state'],
            str(r['ship-postal-code']), r['ship-country']
        )),
        axis=1
    )
    logger.info(f"[LOAD] DIM_SHIPMENTS — {len(records):,} shipment combos processed")
    return df


# ---------------------------------------------------------------------------
# LOAD — fact table
# ---------------------------------------------------------------------------

def run_load(clean_path: str, pg_conn_params: dict) -> int:
    """Upsert dimensions then insert records into FACTS_SALES; return rows inserted."""
    df = pd.read_parquet(clean_path)

    conn = psycopg2.connect(**pg_conn_params)
    cur = conn.cursor()

    try:
        df = _upsert_dim_products(df, cur)
        df = _upsert_dim_date(df, cur)
        df = _upsert_dim_shipments(df, cur)

        # Drop rows where any surrogate key lookup failed
        before = len(df)
        df = df.dropna(subset=['id_product', 'id_date', 'id_shipment'])
        if (dropped := before - len(df)):
            logger.warning(f"[LOAD] {dropped:,} rows dropped — FK lookup failed")

        fact_records = [
            (int(r.id_product), int(r.id_date), int(r.id_shipment),
             float(r.Amount), int(r.Qty), float(r.average_ticket))
            for r in df.itertuples(index=False)
        ]

        execute_values(cur, """
            INSERT INTO FACTS_SALES
                (id_product, id_date, id_shipment, amount, quantity, average_ticket)
            VALUES %s;
        """, fact_records, page_size=500)

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM FACTS_SALES;")
        total = cur.fetchone()[0]
        logger.info(f"[LOAD] FACTS_SALES total rows after insert: {total:,}")
        return total

    except Exception as exc:
        conn.rollback()
        raise RuntimeError(f"[LOAD] Failed — rolled back: {exc}") from exc
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# CLEANUP
# ---------------------------------------------------------------------------

def run_cleanup(*paths: str) -> None:
    """Delete temp parquet files created during the run."""
    for path in paths:
        try:
            os.remove(path)
            logger.info(f"[CLEANUP] Deleted temp file: {path}")
        except FileNotFoundError:
            logger.warning(f"[CLEANUP] File not found, skipping: {path}")