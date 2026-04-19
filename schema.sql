-- DIMENSIONS
CREATE TABLE DIM_PRODUCTS (
    id_product SERIAL PRIMARY KEY,
    sku VARCHAR(100) NOT NULL UNIQUE,
    style VARCHAR(100) NOT NULL,
    category VARCHAR(75) NOT NULL,
    size VARCHAR(30) NOT NULL
);

CREATE TABLE DIM_DATE (
    id_date SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    week_of_year INT NOT NULL
);

CREATE TABLE DIM_SHIPMENTS (
    id_shipment SERIAL PRIMARY KEY,
    ship_service_level VARCHAR(150) NOT NULL,
    ship_city VARCHAR(100) NOT NULL,
    ship_state VARCHAR(100) NOT NULL,
    ship_postal_code VARCHAR(100) NOT NULL,
    ship_country VARCHAR(100) NOT NULL
);

-- TABLE FACTS
CREATE TABLE FACTS_SALES (
    id_venta SERIAL PRIMARY KEY,
    id_product INT NOT NULL,
    id_date INT NOT NULL,
    id_shipment INT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    quantity INTEGER NOT NULL,
    average_ticket DECIMAL(12,2) NOT NULL,

    CONSTRAINT fk_products
        FOREIGN KEY (id_product)
        REFERENCES DIM_PRODUCTS(id_product),

    CONSTRAINT fk_date
        FOREIGN KEY (id_date)
        REFERENCES DIM_DATE(id_date),

    CONSTRAINT fk_shipments
        FOREIGN KEY (id_shipment)
        REFERENCES DIM_SHIPMENTS(id_shipment)
);

-- INDEX
CREATE INDEX idx_ventas_fecha ON fact_ventas(id_date);