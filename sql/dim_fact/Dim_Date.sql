-- 1. Create table only if it doesn't exist (Preserves history)
CREATE TABLE IF NOT EXISTS Dim_Date (
    Date_Key SERIAL PRIMARY KEY,
    Full_Date DATE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT,
    Day_Name VARCHAR(20),
    Month_Name VARCHAR(20)
);

-- 2. Insert ONLY new dates (The "Left Join NULL" Pattern)
INSERT INTO Dim_Date (Full_Date, Year, Month, Day, Quarter, Day_Name, Month_Name)
SELECT DISTINCT 
    s.transaction_date::DATE as Full_Date,
    EXTRACT(YEAR FROM s.transaction_date::DATE) as Year,
    EXTRACT(MONTH FROM s.transaction_date::DATE) as Month,
    EXTRACT(DAY FROM s.transaction_date::DATE) as Day,
    EXTRACT(QUARTER FROM s.transaction_date::DATE) as Quarter,
    TO_CHAR(s.transaction_date::DATE, 'Day') as Day_Name,
    TO_CHAR(s.transaction_date::DATE, 'Month') as Month_Name
FROM staging_order_data s
-- INCREMENTAL CHECK: Match against existing dimension
LEFT JOIN Dim_Date d ON s.transaction_date::DATE = d.Full_Date
-- FILTER: Only keep dates not already in Dim_Date
WHERE s.transaction_date IS NOT NULL 
  AND d.Full_Date IS NULL;

-- 3. Create Index safely
CREATE INDEX IF NOT EXISTS idx_dim_date_full ON Dim_Date(Full_Date);
