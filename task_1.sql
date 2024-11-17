select 
	staff.first_name
	, staff.last_name
	, count(sales_reciepts.transaction_id)
from coffe_shop.sales_reciepts 
	left join coffe_shop.staff 
		using(staff_id)
where 1=1
	and transaction_date = '2019-04-05'
group by 1,2
order by count(sales_reciepts.transaction_id) desc;

WITH daily_revenue AS (
    SELECT 
        sr.transaction_date::date AS transaction_date,
        SUM(sr.quantity * REPLACE(REPLACE(p.current_retail_price, '$', ''), ' ', '')::float) AS sales_amount
    FROM coffe_shop.sales_reciepts sr
    JOIN coffe_shop.product p 
    	ON sr.product_id = p.product_id
    GROUP BY sr.transaction_date::date
)
SELECT 
    transaction_date,
    sales_amount,
    LAG(sales_amount) OVER (ORDER BY transaction_date) AS prev_sales_amount,
    sales_amount - LAG(sales_amount) OVER (ORDER BY transaction_date) AS difference_sales_amount,
    (sales_amount - LAG(sales_amount) OVER (ORDER BY transaction_date)) / 
        NULLIF(LAG(sales_amount) OVER (ORDER BY transaction_date), 0) * 100 AS percent_difference
FROM daily_revenue
ORDER BY 
    transaction_date;