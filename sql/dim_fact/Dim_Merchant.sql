-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Merchant (
    Merchant_Key SERIAL PRIMARY KEY,        
    merchant_id VARCHAR,                
    merchant_name VARCHAR,                    
    merchant_street VARCHAR, 
    merchant_state VARCHAR,          
    merchant_city VARCHAR,                          
    merchant_country VARCHAR,
    merchant_contact_number VARCHAR
);

-- 2. Insert ONLY new merchants (The "Left Join NULL" Pattern)
INSERT INTO Dim_Merchant (
    merchant_id, 
    merchant_name, 
    merchant_street, 
    merchant_state, 
    merchant_city, 
    merchant_country,
    merchant_contact_number
)
SELECT DISTINCT 
    s.merchant_id,
    s.name,
    s.street,
    s.state,
    s.city,
    s.country,
    s.contact_number     
FROM staging_merchant_data s
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Merchant d ON s.merchant_id = d.merchant_id
-- FILTER: Only keep merchants not already in Dim_Merchant
WHERE d.merchant_id IS NULL;