COPY Walmart_Sales_Data FROM 'C:\data analytics\baraa\projects\walmart\WalmartSalesData.csv.csv' 
WITH (FORMAT csv, HEADER true, QUOTE '"');

-- ---------------------------------------------------------------------------------
-- -----------------------------Feature Egineering----------------------------------

-- add new column called time_of_day to find the peek time of the day
SELECT
	time,
	CASE
		WHEN time BETWEEN '00:00:00' AND '11:00:59' THEN 'morning'
		WHEN time BETWEEN '12:00:00' AND '15:59:59' THEN 'afternoon'
		ELSE 'evening'
	END as time_of_day
FROM walmart_sales_data

-- add a new column called week_day to find the busiest day of the week
SELECT 
	date,
	TO_CHAR(date, 'Day') AS day_name
FROM walmart_sales_data

-- add a new column called month_name to find the month with most sales and profit
SELECT 
	date,
	TO_CHAR(date, 'Month') AS month_name
FROM walmart_sales_data


------------------Business Questions To Answer-------------------------------------

------------------Generic Question-------------------------------------------------
--How many unique cities does the data have?
SELECT 
	COUNT(DISTINCT city)
FROM walmart_sales_data

--In which city is each branch?
SELECT 
	city,
	branch
FROM walmart_sales_data
GROUP BY 1, 2

----------------------Product-------------------------------------------------------

--How many unique product lines does the data have?
SELECT
	COUNT(DISTINCT product_line)
FROM walmart_sales_data


--What is the most common payment method?
SELECT 
	payment,
	COUNT(payment) AS common_payment_method
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What is the most selling product line?
SELECT 
	DISTINCT product_line,
	SUM(quantity)
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What is the total revenue by month?
SELECT 
	 TO_CHAR(date, 'Month') AS month_name,
	 SUM(total) AS total_revenue
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What month had the largest COGS?
SELECT 
	TO_CHAR(date, 'Month') AS month_name,
	SUM(cogs) AS total_cogs,
	RANK() OVER (ORDER BY SUM(cogs)DESC) AS ranking
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What product line had the largest revenue?
SELECT 
	product_line,
	SUM(total) AS total_revenue,
	RANK() OVER (ORDER BY SUM(total)DESC) AS ranking
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What is the city with the largest revenue?
SELECT 
	city,
	SUM(total) AS total_revenue,
	RANK() OVER (ORDER BY SUM(total)DESC) AS ranking
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What product line had the largest VAT?
SELECT 
	DISTINCT product_line,
	SUM(VAT) AS total_VAT,
	RANK() OVER (ORDER BY SUM(VAT)DESC) AS ranking
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--Fetch each product line and add a column to those product line showing "Good", "Bad". 
--Good if its greater than average sales
SELECT 
	 product_line,
	ROUND(AVG(total), 2) AS avg_sales,
	CASE
		WHEN AVG(total) > (SELECT AVG(total) FROM walmart_sales_data) THEN 'Good'
		ELSE 'Bad'
	END
FROM walmart_sales_data
GROUP BY 1

--Which branch sold more products than average product sold?
SELECT 
	branch,
	SUM(quantity)
FROM walmart_sales_data
GROUP BY 1 
HAVING SUM(quantity) > (SELECT ROUND(AVG(quantity), 2) FROM walmart_sales_data)
ORDER BY 2 DESC

--What is the most common product line by gender?
SELECT 
gender,
product_line,
number_of_products
FROM
(
	SELECT 
		gender,
		product_line,
		COUNT(*) AS number_of_products,
		ROW_NUMBER () OVER (PARTITION BY gender ORDER BY COUNT(*)DESC) rn
	FROM walmart_sales_data
	GROUP BY 1, 2
) AS ranking
WHERE rn = 1

--What is the average rating of each product line?
SELECT 
	product_line,
	ROUND(AVG(rating), 2) AS rating
FROM walmart_sales_data
GROUP BY 1

-----------------------Sales--------------------------------------------------------

--Number of sales made in each time of the day per weekday
SELECT 
	TO_CHAR(date, 'day') AS week_day,
	SUM(quantity) AS number_of_sales
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--Which of the customer types brings the most revenue?
SELECT *FROM walmart_sales_data

--Which city has the largest tax percent/ VAT (Value Added Tax)?
SELECT 
	city,
	ROUND(AVG(vat/cogs) * 100, 2) AS tax_percent
FROM walmart_sales_data
GROUP BY 1

--Which customer type pays the most in VAT?
SELECT
	customer_type,
	SUM(vat)
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2

----------------------------Customer------------------------------------------------
------------------------------------------------------------------------------------
--How many unique customer types does the data have?
SELECT 
	COUNT(DISTINCT customer_type) AS unique_customers
FROM walmart_sales_data

--How many unique payment methods does the data have?
SELECT 
	COUNT(DISTINCT payment) AS unique_payments
FROM walmart_sales_data

--What is the most common customer type?
SELECT 
	customer_type,
	COUNT(*) AS number_of_cx
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--Which customer type buys the most?
SELECT 
	customer_type,
	SUM(quantity) AS numbers
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC

--What is the gender of most of the customers?
SELECT 
	gender,
	COUNT(*) AS cx_type
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC
limit 1

--What is the gender distribution per branch?
SELECT 
	branch,
	gender,
	COUNT(*) AS cx_distribution
FROM walmart_sales_data
GROUP BY 1, 2
ORDER BY 1

--Which time of the day do customers give most ratings?
SELECT
	EXTRACT(hour FROM time) AS hour_of_day,
	COUNT(rating) AS rating
FROM walmart_sales_data
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1

--Which time of the day do customers give most ratings per branch?
SELECT 
	branch,
	hour_of_day,
	number_rating,
	ranking
FROM 
(
	SELECT 
		 branch, 
		 EXTRACT(hour FROM time) AS hour_of_day,
		 COUNT(rating) AS number_rating,
		 ROW_NUMBER() OVER(PARTITION BY branch ORDER BY COUNT(rating)DESC) ranking
	FROM walmart_sales_data
	GROUP BY 1, 2
)
WHERE ranking = 1

--Which day of the week has the best avg ratings?
SELECT 
	TO_CHAR(date, 'day') AS day_of_week,
	ROUND(AVG(rating), 2) AS average_rating,
	RANK() OVER (ORDER BY ROUND(AVG(rating), 2)DESC)
FROM walmart_sales_data
GROUP BY 1

--Which day of the week has the best average ratings per branch?
SELECT 
	branch,
	day_of_week,
	average_rating,
	ranking
FROM 
(
	SELECT 
		 branch, 
		 TO_CHAR(date, 'day') AS day_of_week,
		 ROUND(AVG(rating), 2) AS average_rating,
		 ROW_NUMBER() OVER(PARTITION BY branch ORDER BY ROUND(AVG(rating), 2)DESC) ranking
	FROM walmart_sales_data
	GROUP BY 1, 2
)
WHERE ranking = 1

--Revenue And Profit Calculations

$ COGS = unitsPrice * quantity $

$ VAT = 5% * COGS $

VAT is added to the COGS and this is what is billed to the customer.
$ total(gross_sales) = VAT + COGS $

$ grossProfit(grossIncome) = total(gross_sales) - COGS $
Gross Margin is gross profit expressed in percentage of the total(gross profit/revenue)

$ \text{Gross Margin} = \frac{\text{gross income}}{\text{total revenue}} $

Example with the first row in our DB:

Data given:

$ \text{Unite Price} = 45.79 $
$ \text{Quantity} = 7 $
$ COGS = 45.79 * 7 = 320.53 $

$ \text{VAT} = 5% * COGS\= 5% 320.53 = 16.0265 $

$ total = VAT + COGS\= 16.0265 + 320.53 = 
336.5565

$ \text{Gross Margin Percentage} = \frac{\text{gross income}}{\text{total revenue}}\=
\frac{16.0265}{336.5565} = 0.047619\\approx 4.7619% $