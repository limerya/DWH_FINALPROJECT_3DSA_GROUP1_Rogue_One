-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Campaign (
    campaign_key SERIAL PRIMARY KEY,    
    campaign_id VARCHAR,                
    campaign_name VARCHAR,
    discount DECIMAL (10,2)                       
);

-- 2. Insert ONLY new campaigns (The "Left Join NULL" Pattern)
INSERT INTO Dim_Campaign (campaign_id, campaign_name, discount)
SELECT DISTINCT 
    s.campaign_id, 
    s.campaign_name, 
    s.discount::DECIMAL (10,2)
FROM staging_campaign_data s
LEFT JOIN Dim_Campaign d ON s.campaign_id = d.campaign_id
WHERE d.campaign_id IS NULL;