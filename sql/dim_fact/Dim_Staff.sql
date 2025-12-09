-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Staff (
    Staff_Key SERIAL PRIMARY KEY,        
    staff_id VARCHAR,                
    staff_name VARCHAR,             
    staff_job_level VARCHAR,        
    staff_street VARCHAR,            
    staff_city VARCHAR,             
    staff_state VARCHAR,            
    staff_country VARCHAR           
);

-- 2. Insert ONLY new staff members (The "Left Join NULL" Pattern)
INSERT INTO Dim_Staff (
    staff_id, 
    staff_name, 
    staff_job_level, 
    staff_street, 
    staff_city, 
    staff_state, 
    staff_country
)
SELECT DISTINCT 
    s.staff_id, 
    s.name,           
    s.job_level,    
    s.street,         
    s.city,           
    s.state,          
    s.country         
FROM staging_staff_data s
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Staff d ON s.staff_id = d.staff_id
-- FILTER: Only keep staff not already in Dim_Staff
WHERE d.staff_id IS NULL;