CREATE DATABASE tesla_financials;
USE tesla_financials;


CREATE TABLE tesla_stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT
);


CREATE TABLE tesla_quarterly_financials (
    id INT AUTO_INCREMENT PRIMARY KEY,
    quarter VARCHAR(10) NOT NULL,   -- e.g., 'Q1 2024'
    revenue_billion DECIMAL(10,2),
    net_income_billion DECIMAL(10,2),
    eps DECIMAL(10,2)
);



CREATE TABLE tesla_production_sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    quarter VARCHAR(10) NOT NULL,
    model VARCHAR(50),
    production_units INT,
    delivery_units INT
);

CREATE TABLE tesla_financials (
    Year INT PRIMARY KEY,
    Revenue_Billion DECIMAL(10, 2),
    Profit_Billion DECIMAL(10, 2)
);

INSERT INTO tesla_financials (Year, Revenue_Billion, Profit_Billion) VALUES
(2015, 35.76, -1.45),
(2016, 42.13, 0.58),
(2017, 48.90, 1.72),
(2018, 60.22, 2.45),
(2019, 70.10, 3.90),
(2020, 81.43, 5.22),
(2021, 92.18, 7.56),
(2022, 97.55, 9.12),
(2023, 88.22, 6.87),
(2024, 91.33, 10.44);


-- Queries
-- Calculate Total Revenue and Profit
Select sum(Revenue_billion) as Total_Revenue,  
 sum(Profit_Billion) as Total_Profit
from tesla_financials;



-- Find the Most Profitable Year
Select Year as Profitable_Year, 
sum(Profit_Billion) as Total_Profit
from tesla_financials
Group by Year
Order by Profitable_Year DESC;


-- Calculate Average Revenue and Profit
Select Round(Avg(Revenue_Billion),2) as Avg_Revenue,
Round(Avg(Profit_Billion),2) as Avg_Profit
From tesla_financials;


-- Year-over-Year Growth 
SELECT 
  Year,
  SUM(Revenue_Billion) AS Revenue_Billion,
  SUM(Profit_Billion) AS Profit_Billion,
  
  ROUND(
    100.0 * (SUM(Revenue_Billion) - LAG(SUM(Revenue_Billion)) OVER (ORDER BY Year)) 
    / NULLIF(LAG(SUM(Revenue_Billion)) OVER (ORDER BY Year), 0),
    2
  ) AS Revenue_YoY_Growth_Percent,

  ROUND(
    100.0 * (SUM(Profit_Billion) - LAG(SUM(Profit_Billion)) OVER (ORDER BY Year)) 
    / NULLIF(LAG(SUM(Profit_Billion)) OVER (ORDER BY Year), 0),
    2
  ) AS Profit_YoY_Growth_Percent

FROM tesla_financials
GROUP BY Year
ORDER BY Year;
