CREATE TABLE mock_data (
    id INT,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INT,
    customer_email VARCHAR,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price DECIMAL,
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price DECIMAL,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR,
    pet_category VARCHAR,
    product_weight DECIMAL,
    product_color VARCHAR,
    product_size VARCHAR,
    product_brand VARCHAR,
    product_material VARCHAR,
    product_description TEXT,
    product_rating DECIMAL,
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR
);

COPY mock_data FROM '/mock_data/MOCK_DATA.csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (1).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (2).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (3).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (4).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (5).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (6).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (7).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (8).csv' WITH CSV HEADER;
COPY mock_data FROM '/mock_data/MOCK_DATA (9).csv' WITH CSV HEADER;

ALTER TABLE mock_data DROP COLUMN id;
ALTER TABLE mock_data ADD COLUMN id SERIAL PRIMARY KEY;
