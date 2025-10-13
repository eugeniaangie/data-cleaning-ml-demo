-- Database schema untuk coffee shop data management
-- Run: psql -U postgres -d coffee_shop_db -f database_schema.sql

-- Table 1: Master Data Lokasi
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    address TEXT,
    area_sqm DECIMAL(10, 2),
    rating DECIMAL(3, 2),
    followers INTEGER DEFAULT 0,
    data_hash VARCHAR(64) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Data Harga (History)
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
    monthly_rent DECIMAL(15, 2),
    price_per_sqm DECIMAL(10, 2),
    date_recorded DATE DEFAULT CURRENT_DATE,
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Social Media Metrics
CREATE TABLE IF NOT EXISTS social_metrics (
    id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
    platform VARCHAR(50) NOT NULL,
    followers INTEGER DEFAULT 0,
    engagement_rate DECIMAL(5, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: Duplicate Detection Log
CREATE TABLE IF NOT EXISTS duplicate_log (
    id SERIAL PRIMARY KEY,
    location_id_1 INTEGER REFERENCES locations(id),
    location_id_2 INTEGER REFERENCES locations(id),
    similarity_score DECIMAL(5, 2),
    distance_meters DECIMAL(10, 2),
    action_taken VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes untuk performance
CREATE INDEX IF NOT EXISTS idx_locations_coordinates ON locations(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_locations_name ON locations(name);
CREATE INDEX IF NOT EXISTS idx_locations_hash ON locations(data_hash);
CREATE INDEX IF NOT EXISTS idx_prices_location_id ON prices(location_id);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date_recorded);
CREATE INDEX IF NOT EXISTS idx_social_location_id ON social_metrics(location_id);

-- Function untuk update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger untuk auto-update timestamp
CREATE TRIGGER update_locations_updated_at 
    BEFORE UPDATE ON locations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
