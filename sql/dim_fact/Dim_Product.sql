-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Product (
    Product_Key SERIAL PRIMARY KEY,       
    product_id VARCHAR(255),              
    product_name VARCHAR(255),            
    product_type VARCHAR(255),            
    price DECIMAL(10, 2)                  
);

-- 2. Insert ONLY new products (The "Left Join NULL" Pattern)
INSERT INTO Dim_Product (product_id, product_name, product_type, price)
SELECT DISTINCT 
    s.product_id, 
    s.product_name, 
    s.product_type, 
    s.price::DECIMAL(10,2)       
FROM staging_product_list s
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Product d ON s.product_id = d.product_id
-- FILTER: Only keep products not already in Dim_Product
WHERE d.product_id IS NULL;