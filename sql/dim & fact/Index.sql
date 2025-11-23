--zipper join 
CREATE INDEX IF NOT EXISTS idx_sliprod_order ON staging_line_item_products(order_id);
CREATE INDEX IF NOT EXISTS idx_sliprice_order ON staging_line_item_prices(order_id);
CREATE INDEX IF NOT EXISTS idx_sod_order ON staging_order_data(order_id);
CREATE INDEX IF NOT EXISTS idx_somd_order ON staging_order_with_merchant_data(order_id);
CREATE INDEX IF NOT EXISTS idx_stcd_order ON staging_transactional_campaign_data(order_id);

--for dimesnions
CREATE INDEX IF NOT EXISTS idx_sliprod_product ON staging_line_item_products(product_id);
CREATE INDEX IF NOT EXISTS idx_sod_user ON staging_order_data(user_id);
CREATE INDEX IF NOT EXISTS idx_somd_merchant ON staging_order_with_merchant_data(merchant_id);
CREATE INDEX IF NOT EXISTS idx_somd_staff ON staging_order_with_merchant_data(staff_id);
CREATE INDEX IF NOT EXISTS idx_stcd_campaign ON staging_transactional_campaign_data(campaign_id);

--consult kay sir if pwede to or aligned pa ba sa work natin pero kasi
--pag wala to umabot 15 mins query ayaw pa rin mainsert naka pc na ko 