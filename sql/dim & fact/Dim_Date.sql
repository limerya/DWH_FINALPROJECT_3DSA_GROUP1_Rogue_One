DROP TABLE IF EXISTS Dim_Date CASCADE;

CREATE TABLE Dim_Date (
    Date_Key SERIAL PRIMARY KEY,
    Full_Date DATE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT,
    Day_Name VARCHAR(20),
    Month_Name VARCHAR(20)
);

-- Insert distinct dates from the staging order data
INSERT INTO Dim_Date (Full_Date, Year, Month, Day, Quarter, Day_Name, Month_Name)
SELECT DISTINCT 
    transaction_date::DATE as Full_Date,
    EXTRACT(YEAR FROM transaction_date::DATE) as Year,
    EXTRACT(MONTH FROM transaction_date::DATE) as Month,
    EXTRACT(DAY FROM transaction_date::DATE) as Day,
    EXTRACT(QUARTER FROM transaction_date::DATE) as Quarter,
    TO_CHAR(transaction_date::DATE, 'Day') as Day_Name,
    TO_CHAR(transaction_date::DATE, 'Month') as Month_Name
FROM staging_order_data
WHERE transaction_date IS NOT NULL;

-- Index
CREATE INDEX idx_dim_date_full ON Dim_Date(Full_Date);

SELECT * FROM Dim_Date;
