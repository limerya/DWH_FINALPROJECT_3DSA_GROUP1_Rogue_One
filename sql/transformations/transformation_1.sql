UPDATE staging_campaign_data
SET discount = SUBSTRING(discount FROM '^[0-9]+');

ALTER TABLE staging_campaign_data
ALTER COLUMN discount TYPE NUMERIC(10,2) 
USING discount::NUMERIC / 100.0;