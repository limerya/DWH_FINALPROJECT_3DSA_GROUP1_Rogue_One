DROP TABLE IF EXISTS Dim_Order CASCADE;

-- dim_order
CREATE TABLE Dim_Order (
    Order_Key SERIAL PRIMARY KEY,
    Order_ID VARCHAR(255),
    Estimated_Arrival VARCHAR(50), 
    Availed INT                    
);

-- Insert order details via JOIN Order Data + Campaign Data
INSERT INTO Dim_Order (Order_ID, Estimated_Arrival, Availed)
SELECT DISTINCT 
    sod.order_id,
    sod.estimated_arrival,
    -- handle nulls
    COALESCE(stcd.availed::INTEGER, 0) 
FROM staging_order_data sod
LEFT JOIN staging_transactional_campaign_data stcd 
    ON sod.order_id = stcd.order_id;

-- Index 
CREATE INDEX idx_dim_order_natural_key ON Dim_Order(Order_ID);
---------------------------------------------------------------