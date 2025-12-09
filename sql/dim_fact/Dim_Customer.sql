-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Customer (
    Customer_Key SERIAL PRIMARY KEY,      
    user_id VARCHAR,                 
    customer_name VARCHAR,
    job_title VARCHAR,
    job_level VARCHAR,
    credit_card_number VARCHAR,
    issuing_bank VARCHAR,
    customer_gender VARCHAR,          
    customer_street VARCHAR,         
    customer_state VARCHAR,          
    customer_city VARCHAR,           
    customer_country VARCHAR,        
    customer_user_type VARCHAR        
);

-- 2. Insert ONLY new customers (The "Left Join NULL" Pattern)
INSERT INTO Dim_Customer (
    user_id, 
    customer_name, 
    job_title, 
    job_level, 
    credit_card_number, 
    issuing_bank, 
    customer_gender, 
    customer_street, 
    customer_state, 
    customer_city, 
    customer_country, 
    customer_user_type
)
SELECT DISTINCT
    sd.user_id,                
    sd.name,                   
    sj.job_title,              
    sj.job_level,               
    scc.credit_card_number,     
    scc.issuing_bank,           
    sd.gender,                  
    sd.street,                 
    sd.state,                   
    sd.city,                    
    sd.country,                 
    sd.user_type                
FROM staging_user_data AS sd
LEFT JOIN staging_user_job AS sj 
    ON sd.user_id = sj.user_id
LEFT JOIN staging_user_credit_card AS scc 
    ON sd.user_id = scc.user_id
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Customer AS d 
    ON sd.user_id = d.user_id
-- FILTER: Only keep rows where the user_id was NOT found in Dim_Customer
WHERE d.user_id IS NULL;