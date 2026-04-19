import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# -- Load Data ---
PATH = 'data/Amazon Sale Report.csv'
df = pd.read_csv(PATH)

# --- Cleaning Data ---
df.drop_duplicates(inplace=True)
df = df.dropna(subset=['Amount'])
df = df[df['Qty'] > 0]

cols_to_fix = ['ship-city', 'ship-state', 'ship-postal-code', 'ship-country']
for col in cols_to_fix:
    df[col] = df[col].fillna('Unknown')

# --- Data Transform ---
df['Date'] = pd.to_datetime(df['Date'])
df['month'] = df['Date'].dt.month
df['week_of_year'] = df['Date'].dt.isocalendar().week
df['quarter'] = df['Date'].dt.quarter
df['day'] = df['Date'].dt.day
df['year'] = df['Date'].dt.year

df['average_ticket'] = df['Amount'] / df['Qty']

# --- PostgreSQL Connection and Load ---
def load_to_postgres(dataframe):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS')
        )
        cur = conn.cursor()

        # 1. Load DIM_PRODUCTS
        products = dataframe[['SKU', 'Style', 'Category', 'Size']].drop_duplicates()
        product_data = [tuple(x) for x in products.values]
        execute_values(cur, """
            INSERT INTO DIM_PRODUCTS (sku, style, category, size) 
            VALUES %s ON CONFLICT (sku) DO NOTHING
        """, product_data)

        # 2. Load DIM_DATE
        dates = dataframe[['Date', 'day', 'month', 'quarter', 'year', 'week_of_year']].drop_duplicates()
        date_data = [(x[0].date(), x[1], x[2], x[3], x[4], x[5]) for x in dates.values]
        execute_values(cur, """
            INSERT INTO DIM_DATE (full_date, day, month, quarter, year, week_of_year) 
            VALUES %s
        """, date_data)

        # 3. Load DIM_SHIPMENTS
        shipments = dataframe[['ship-service-level', 'ship-city', 'ship-state', 'ship-postal-code', 'ship-country']].drop_duplicates()
        shipment_data = [tuple(x) for x in shipments.values]
        execute_values(cur, """
            INSERT INTO DIM_SHIPMENTS (ship_service_level, ship_city, ship_state, ship_postal_code, ship_country) 
            VALUES %s
        """, shipment_data)

        conn.commit() # Aseguramos dimensiones antes de mapear hechos

        # --- Load FACTS_SALES ---
        cur.execute("SELECT id_product, sku FROM DIM_PRODUCTS")
        prod_map = {sku: id_prod for id_prod, sku in cur.fetchall()}

        cur.execute("SELECT id_date, full_date FROM DIM_DATE")
        date_map = {full_date: id_date for id_date, full_date in cur.fetchall()}

        cur.execute("SELECT id_shipment, ship_city, ship_postal_code FROM DIM_SHIPMENTS")
        ship_map = {(city, str(postal)): id_ship for id_ship, city, postal in cur.fetchall()}

        fact_records = []
        total = len(dataframe)

        print(f"\n STARTING SALES MAPPING AND LOADING...")
        print(f"Total records: {total}")

        # Bucle con Log de progreso
        for i, (index, row) in enumerate(dataframe.iterrows(), 1):
            id_prod = prod_map.get(row['SKU'])
            id_date = date_map.get(row['Date'].date())
            id_ship = ship_map.get((row['ship-city'], str(row['ship-postal-code'])))

            if id_prod and id_date and id_ship:
                fact_records.append((id_prod, id_date, id_ship, row['Amount'], row['Qty'], row['average_ticket']))

            if i % 5000 == 0 or i == total:
                porcentaje = (i / total) * 100
                print(f"Read: {i}/{total} ({porcentaje:.2f}%) | Mapped: {len(fact_records)}")

        # Carga masiva única de hechos
        print("Sending Data to Postgres...")
        execute_values(cur, """
            INSERT INTO FACTS_SALES (id_product, id_date, id_shipment, amount, quantity, average_ticket)
            VALUES %s
        """, fact_records)

        conn.commit()

        # Reporte Final
        print("\n--- FINAL REPORT ---")
        tables = ['DIM_PRODUCTS', 'DIM_DATE', 'DIM_SHIPMENTS', 'FACTS_SALES']
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"{table}: {cur.fetchone()[0]} uploaded records.")

    except Exception as e:
        print(f"Error during loading: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    load_to_postgres(df)