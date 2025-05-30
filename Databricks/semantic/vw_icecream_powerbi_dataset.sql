-- Single comprehensive view for Power BI consumption
CREATE OR REPLACE VIEW icecreamdb.semantic.vw_icecream_powerbi_dataset AS
SELECT
    -- ===========================================
    -- FACT TABLE IDENTIFIERS & KEYS
    -- ===========================================
    fs.sales_sk AS fact_sales_key,
    fs.date_sk,
    fs.product_sk,
    fs.country_sk,
    
    -- ===========================================
    -- DATE DIMENSION ATTRIBUTES
    -- ===========================================
    fs.sales_date,
    dd.calendar_year AS sales_year,
    dd.calendar_month AS sales_month,
    dd.calendar_month_name AS sales_month_name,
    dd.calendar_day AS sales_day,
    dd.calendar_quarter AS sales_quarter,
    dd.week_of_year AS sales_week_of_year,
    dd.day_of_week AS sales_day_of_week,
    dd.day_of_week_name AS sales_day_name,
    
    -- ===========================================
    -- PRODUCT DIMENSION ATTRIBUTES
    -- ===========================================
    fs.product_code,
    dp.product_name,
    dp.price AS current_product_price,
    
    -- ===========================================
    -- COUNTRY DIMENSION ATTRIBUTES  
    -- ===========================================
    dc.country_name AS country,
    dc.country_index AS big_mac_index,
    dc.average_price AS big_mac_average_price,
    
    -- ===========================================
    -- RAW SALES MEASURES (for Power BI calculations)
    -- ===========================================
    fs.total_sales_amount,
    fs.transaction_count,
    fs.sales_qty_amnt AS adjusted_sales_quantity,
    fs.sales_value_amnt AS adjusted_sales_value,
    
    -- ===========================================
    -- WEATHER MEASURES
    -- ===========================================
    fs.avg_temp_c AS temperature_celsius,
    fs.avg_temp_f AS temperature_fahrenheit,
    fs.avg_humidity AS humidity_percentage,
    fs.avg_precip_mm AS precipitation_mm,
    fs.avg_wind_kph AS wind_speed_kph,
    fs.avg_pressure_mb AS atmospheric_pressure_mb,
    fs.avg_cloud_cover AS cloud_cover_percentage,
    fs.avg_uv_index AS uv_index,
    
    -- ===========================================
    -- RENTAL COST INFORMATION
    -- ===========================================
    fs.rental_price AS base_monthly_rental_eur,
    fs.rental_month,
    fs.rental_year,
    
    -- Mercedes Sprinter adjusted rental cost (base * 1.5)
    fs.rental_price * 1.5 AS sprinter_monthly_rental_eur,
    
    -- ===========================================
    -- BUSINESS CATEGORIZATION (Simple)
    -- ===========================================
    
    -- Season
    CASE 
        WHEN dd.calendar_month IN (12, 1, 2) THEN 'Winter'
        WHEN dd.calendar_month IN (3, 4, 5) THEN 'Spring'
        WHEN dd.calendar_month IN (6, 7, 8) THEN 'Summer'
        WHEN dd.calendar_month IN (9, 10, 11) THEN 'Autumn'
        ELSE 'Unknown'
    END AS season,
    
    -- Day Type
    CASE 
        WHEN dd.day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    
    -- Basic Weather Categories (for filtering in Power BI)
    CASE 
        WHEN fs.avg_temp_c IS NULL THEN 'No Data'
        WHEN fs.avg_temp_c < 0 THEN 'Freezing'
        WHEN fs.avg_temp_c BETWEEN 0 AND 10 THEN 'Cold'
        WHEN fs.avg_temp_c BETWEEN 11 AND 20 THEN 'Cool'
        WHEN fs.avg_temp_c BETWEEN 21 AND 30 THEN 'Warm'
        WHEN fs.avg_temp_c > 30 THEN 'Hot'
        ELSE 'Unknown'
    END AS temperature_category,
    
    CASE 
        WHEN fs.avg_humidity IS NULL THEN 'No Data'
        WHEN fs.avg_humidity < 40 THEN 'Low Humidity'
        WHEN fs.avg_humidity BETWEEN 40 AND 70 THEN 'Moderate Humidity'
        WHEN fs.avg_humidity > 70 THEN 'High Humidity'
        ELSE 'Unknown'
    END AS humidity_category,
    
    CASE 
        WHEN fs.avg_precip_mm IS NULL THEN 'No Data'
        WHEN fs.avg_precip_mm = 0 THEN 'No Rain'
        WHEN fs.avg_precip_mm BETWEEN 0.1 AND 2.5 THEN 'Light Rain'
        WHEN fs.avg_precip_mm BETWEEN 2.6 AND 10 THEN 'Moderate Rain'
        WHEN fs.avg_precip_mm > 10 THEN 'Heavy Rain'
        ELSE 'Unknown'
    END AS precipitation_category,
    
    -- Basic Product Price Category
    CASE 
        WHEN dp.price < 5.00 THEN 'Budget'
        WHEN dp.price BETWEEN 5.00 AND 10.00 THEN 'Standard'
        WHEN dp.price > 10.00 THEN 'Premium'
        ELSE 'Unknown'
    END AS product_price_category,
    
    -- Basic Market Cost Category
    CASE 
        WHEN dc.country_index < 80 THEN 'Low Cost Market'
        WHEN dc.country_index BETWEEN 80 AND 120 THEN 'Average Cost Market'
        WHEN dc.country_index > 120 THEN 'High Cost Market'
        ELSE 'Unknown Market'
    END AS market_cost_category,
    
    -- Basic Rental Cost Category
    CASE 
        WHEN fs.rental_price IS NULL THEN 'No Data'
        WHEN fs.rental_price < 1000 THEN 'Low Rent'
        WHEN fs.rental_price BETWEEN 1000 AND 2500 THEN 'Moderate Rent'
        WHEN fs.rental_price > 2500 THEN 'High Rent'
        ELSE 'Unknown'
    END AS rental_cost_category,
    
    -- ===========================================
    -- CONSTANT VALUES FOR POWER BI CALCULATIONS
    -- ===========================================
    
    -- Transportation constants
    200 AS avg_distance_between_countries_km,
    10 AS km_per_liter_diesel_efficiency,
    1.40 AS diesel_price_per_liter_eur,
    
    -- Business rule constants
    3 AS revenue_to_rent_multiplier_threshold,
    1.5 AS sprinter_rental_multiplier,
    25 AS optimal_temperature_celsius,
    
    -- ===========================================
    -- DATA QUALITY & LINEAGE
    -- ===========================================
    fs.processing_date AS fact_processing_date,
    fs.processing_timestamp AS fact_processing_timestamp,
    fs.processed_by,
    
    -- Data completeness flags (simple boolean for Power BI)
    CASE WHEN fs.avg_temp_c IS NOT NULL THEN 1 ELSE 0 END AS has_weather_data,
    CASE WHEN fs.rental_price IS NOT NULL THEN 1 ELSE 0 END AS has_rental_data,
    CASE WHEN fs.sales_value_amnt IS NOT NULL THEN 1 ELSE 0 END AS has_complete_sales_data,
    
    -- ===========================================
    -- HELPFUL DERIVED FIELDS (Simple calculations only)
    -- ===========================================
    
    -- Year-Month for easy grouping
    CONCAT(dd.calendar_year, '-', LPAD(dd.calendar_month, 2, '0')) AS year_month,
    
    -- Quarter-Year for seasonal analysis
    CONCAT(dd.calendar_year, '-Q', dd.calendar_quarter) AS year_quarter,
    
    -- Is Weekend flag
    CASE WHEN dd.day_of_week IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    
    -- Is Summer flag (key season for ice cream)
    CASE WHEN dd.calendar_month IN (6, 7, 8) THEN 1 ELSE 0 END AS is_summer,
    
    -- Temperature above optimal flag
    CASE WHEN fs.avg_temp_c > 25 THEN 1 ELSE 0 END AS is_optimal_temperature,
    
    -- No precipitation flag
    CASE WHEN COALESCE(fs.avg_precip_mm, 0) = 0 THEN 1 ELSE 0 END AS is_no_rain,
    
    -- High market index flag
    CASE WHEN dc.country_index > 100 THEN 1 ELSE 0 END AS is_high_cost_market

FROM icecreamdb.gold.fact_sales fs

-- Join with Date Dimension
INNER JOIN icecreamdb.gold.dim_date dd
    ON fs.date_sk = dd.date_sk

-- Join with Product Dimension (SCD Type 2 - current version)
INNER JOIN icecreamdb.gold.dim_product dp
    ON fs.product_sk = dp.product_sk

-- Join with Country Dimension (SCD Type 2 - current version)  
INNER JOIN icecreamdb.gold.dim_country dc
    ON fs.country_sk = dc.country_sk

-- Add view metadata
;