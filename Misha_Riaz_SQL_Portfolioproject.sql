
-- -----------------------------------------------------
-- ---------Customer Nodes Exploration------------------
-- -----------------------------------------------------

-- How many unique nodes are there on the Data Bank system?

SELECT COUNT(DISTINCT node_id) AS unique_nodes_count
FROM customer_nodes;

-- What is the number of nodes per region?

SELECT r.region_name, COUNT(cn.node_id) AS nodes_per_region
FROM regions r
JOIN customer_nodes cn ON r.region_id = cn.region_id
GROUP BY r.region_name;

--  How many customers are allocated to each region?

SELECT r.region_name, COUNT(DISTINCT cn.customer_id) AS customers_per_region
FROM regions r
JOIN customer_nodes cn ON r.region_id = cn.region_id
GROUP BY r.region_name;

-- How many days on average are customers reallocated to a different node?

SELECT AVG(DATEDIFF(end_date, start_date)) AS avg_days_reallocation
FROM customer_nodes
WHERE DATEDIFF(end_date, start_date) < 365;

SELECT node_id,
AVG(DATEDIFF(end_date, start_date)) AS avg_days_reallocation
FROM customer_nodes
WHERE end_date IS NOT NULL AND YEAR (end_date) <> 9999
GROUP BY node_id
ORDER BY node_id;

-- What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

with rows_ as (
select c.customer_id,
r.region_name, DATEDIFF(c.end_date, c.start_date) AS days_difference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_date)) AS rows_number,
COUNT(*) over (partition by r.region_name) as total_rows  
from
customer_nodes c JOIN regions r ON c.region_id = r.region_id
where c.end_date not like '%9999%'
)
SELECT region_name,
ROUND(AVG(CASE WHEN rows_number between (total_rows/2) and ((total_rows/2)+1) THEN days_difference END), 0) AS Median,
MAX(CASE WHEN rows_number = round((0.80 * total_rows),0) THEN days_difference END) AS 80th_Percentile,
MAX(CASE WHEN rows_number = round((0.95 * total_rows),0) THEN days_difference END) AS 95th_Percentile
from rows_
group by region_name;

-- -----------------------------------------------------
-- --------------Customer Transactions------------------
-- -----------------------------------------------------

-- What is the unique count and total amount for each transaction type?

SELECT txn_type, COUNT(DISTINCT customer_id) AS unique_count, SUM(txn_amount) AS total_amount
FROM customer_transactions
GROUP BY txn_type;

-- What is the average total historical deposit counts and amounts for all customers?

SELECT 
    AVG(total_deposit_counts) AS average_deposit_counts,
    AVG(total_deposit_amounts) AS average_deposit_amounts
FROM (
    SELECT 
        customer_id,
        COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) AS total_deposit_counts,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount END) AS total_deposit_amounts
    FROM customer_transactions
    GROUP BY customer_id
) AS customer_totals;

-- For each month- how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?

SELECT YEAR(txn_date) AS year, MONTH(txn_date) AS month, COUNT(customer_id) AS customers_count
FROM customer_transactions
WHERE txn_type IN ('deposit', 'purchase', 'withdrawal')
GROUP BY YEAR(txn_date), MONTH(txn_date), customer_id
HAVING 
    SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) > 1
    AND (SUM(CASE WHEN txn_type = 'purchase' THEN 1 ELSE 0 END) = 1
         OR SUM(CASE WHEN txn_type = 'withdrawal' THEN 1 ELSE 0 END) = 1)
ORDER BY year, month;

-- What is the closing balance for each customer at the end of the month?
SELECT 
    customer_id,
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    SUM(txn_amount) AS closing_balance
FROM 
    customer_transactions
GROUP BY 
    customer_id,
    YEAR(txn_date),
    MONTH(txn_date)
ORDER BY year, month;

-- What is the percentage of customers who increase their closing balance by more than 5%?

SELECT 
    (COUNT(DISTINCT b1.customer_id) * 100.0 / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions)) AS percentage,
    COUNT(DISTINCT b1.customer_id) AS count_customer
FROM (
    SELECT 
        customer_id,
        YEAR(txn_date) AS year,
        MONTH(txn_date) AS month,
        SUM(txn_amount) AS closing_balance
    FROM 
        customer_transactions
    GROUP BY 
        customer_id,
        YEAR(txn_date),
        MONTH(txn_date)
) AS b1
JOIN (
    SELECT 
        customer_id,
        YEAR(txn_date) AS year,
        MONTH(txn_date) AS month,
        SUM(txn_amount) AS closing_balance
    FROM 
        customer_transactions
    GROUP BY 
        customer_id,
        YEAR(txn_date),
        MONTH(txn_date)
) AS b2 ON b1.customer_id = b2.customer_id
        AND (b1.year > b2.year OR (b1.year = b2.year AND b1.month > b2.month))
WHERE 
    (b1.closing_balance - b2.closing_balance) / b2.closing_balance > 0.05;

-- -----------------------------------------------------
-- ------------Data Allocation Challenge----------------
-- -----------------------------------------------------

--  Option 1: Data is allocated based off the amount of money at the end of the previous month?

SET SQL_mode = '';

WITH adjusted_amount AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM (txn_date)) AS month_number, 
MONTHNAME(txn_date) AS month,
CASE 
WHEN  txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY month_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
AS running_balance
FROM adjusted_amount
),
allocation AS (
SELECT customer_id, month_number,month,
LAG(running_balance,1) OVER(PARTITION BY customer_id, month_number ORDER BY month_number) AS monthly_allocation
FROM balance
)
SELECT month_number,month,
SUM(CASE WHEN monthly_allocation < 0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM allocation
GROUP BY 1,2
ORDER BY 1,2;
 
-- Option 2: Data is allocated on the average amount of money kept in the account in the previous 30 days

WITH updated_transactions AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM(txn_date)) AS Month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month, month_number,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number 
ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
),

avg_running AS(
SELECT customer_id, month,month_number,
AVG(running_balance) AS avg_balance
FROM balance
GROUP BY 1,2,3
ORDER BY 1
)
SELECT month_number,month, 
SUM(CASE WHEN avg_balance < 0 THEN 0 ELSE avg_balance END) AS allocation_balance
FROM avg_running
GROUP BY 1,2
ORDER by 1,2;

-- Option 3: Data is updated real-time

WITH updated_transactions AS (
SELECT customer_id, txn_type,
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month, 
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number ASC 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
)
SELECT month_number, month,
SUM(CASE WHEN running_balance < 0 THEN 0 ELSE running_balance END) AS total_allocation
FROM balance
GROUP BY 1,2
ORDER BY 1;

-- -------------------------------------------------------
-- ---------------Extra Challenge-------------------------
-- -------------------------------------------------------
-- Calculate the daily data growth for each customer

WITH total_allocation AS ( SELECT month_number, month, SUM(CASE WHEN monthly_allocation < 0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM (SELECT EXTRACT(MONTH FROM (txn_date)) AS month_number, MONTHNAME(txn_date) AS month,
CASE WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS monthly_allocation
FROM customer_transactions) AS allocation
GROUP BY month_number, month
ORDER BY month_number, month),
daily_data_growth AS ( SELECT month_number, month, (total_allocation * 0.06 / 365) AS daily_data_growth
FROM total_allocation)
SELECT d.month_number, d.month, SUM(d.daily_data_growth) AS monthly_data_growth
FROM daily_data_growth d
GROUP BY d.month_number, d.month
ORDER BY d.month_number, d.month;







