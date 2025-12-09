-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Order (
    Order_Key SERIAL PRIMARY KEY,
    Order_ID VARCHAR(255),
    Estimated_Arrival VARCHAR(50), 
    Availed INT                    
);

-- 2. Insert ONLY new orders (The "Left Join NULL" Pattern)
INSERT INTO Dim_Order (Order_ID, Estimated_Arrival, Availed)
SELECT DISTINCT 
    sod.order_id,
    sod.estimated_arrival,
    -- handle nulls
    COALESCE(stcd.availed::INTEGER, 0) 
FROM staging_order_data sod
LEFT JOIN staging_transactional_campaign_data stcd 
    ON sod.order_id = stcd.order_id
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Order d ON sod.order_id = d.Order_ID
-- FILTER: Only keep orders not already in Dim_Order
WHERE d.Order_ID IS NULL;

-- 3. Create Index safely
CREATE INDEX IF NOT EXISTS idx_dim_order_natural_key ON Dim_Order(Order_ID);