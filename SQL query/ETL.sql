create database market_data
use market_data;

create table market_data (
    Year INT,
    Super_Region VARCHAR(255),
    Region VARCHAR(255),
    Country VARCHAR(255),
    Vendor VARCHAR(255),
    Service_1 VARCHAR(255),
    Service_2 VARCHAR(255),
    Service_3 VARCHAR(255),
    Vertical VARCHAR(255),
    Ticker VARCHAR(255),
    HQ_Country VARCHAR(255),
    VendorRevenue_USD DECIMAL(20, 7),
    ConstantCurrency_Revenue_USD DECIMAL(20, 7)
); 
 
 select* from market_data;
select count(*) from market_data;

# Data Validation 
SELECT COUNT(*) 
FROM market_data 
WHERE Year IS NULL or Super_Region is null or Region is null or Vendor is null or Service_1 is null or Service_2 is null or Service_3 is null or Vertical is null or Ticker is null or HQ_Country is null or VendorRevenue_USD is null or ConstantCurrency_Revenue_USD is null;

SELECT Year, Vendor, Country, Service_1, Service_2, Service_3, Vertical, Ticker, HQ_Country, VendorRevenue_USD, ConstantCurrency_Revenue_USD, COUNT(*) as Duplicate_Count
FROM market_data
GROUP BY Year, Vendor, Country, Service_1, Service_2, Service_3, Vertical,Ticker, HQ_Country, VendorRevenue_USD, ConstantCurrency_Revenue_USD
HAVING COUNT(*) > 1
limit 1000;

# Performance  Optimization
EXPLAIN 
SELECT Year, Vendor, Country, Service_1, Service_2, Service_3, Vertical, Ticker, HQ_Country, 
       VendorRevenue_USD, ConstantCurrency_Revenue_USD, COUNT(*) AS Duplicate_Count
FROM market_data
GROUP BY Year, Vendor, Country, Service_1, Service_2, Service_3, Vertical, Ticker, HQ_Country, 
         VendorRevenue_USD, ConstantCurrency_Revenue_USD
HAVING COUNT(*) > 1;

CREATE INDEX idx_year ON market_data (Year);
CREATE INDEX idx_vendor_country ON market_data (Vendor, Country);
CREATE INDEX idx_revenue ON market_data (VendorRevenue_USD);
CREATE INDEX idx_currencyrevenue ON market_data (ConstantCurrency_Revenue_USD);
SHOW INDEXES FROM market_data;

SHOW TABLE STATUS LIKE 'mmarket_data';

SHOW VARIABLES LIKE 'max_allowed_packet';
SHOW VARIABLES LIKE 'wait_timeout';
SHOW VARIABLES LIKE 'innodb_buffer_pool_size';

OPTIMIZE TABLE market_data;
ANALYZE TABLE market_data;


# key bottlenecks
SELECT Year, Vendor, SUM(VendorRevenue_USD) 
FROM market_data 
GROUP BY Year, Vendor 
ORDER BY Year;

SELECT 
    Vendor, 
    AVG(VendorRevenue_USD) AS avg_revenue, 
    MAX(VendorRevenue_USD) AS max_revenue, 
    MIN(VendorRevenue_USD) AS min_revenue
FROM market_data
GROUP BY Vendor
ORDER BY max_revenue DESC
LIMIT 100;




SET SQL_SAFE_UPDATES = 0;
SET GLOBAL max_allowed_packet = 1073741824; 
SET GLOBAL wait_timeout = 28800;              
SET GLOBAL net_read_timeout = 600;
SET GLOBAL net_write_timeout = 600;
SET GLOBAL interactive_timeout = 28800;

