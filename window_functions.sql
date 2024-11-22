/*
1. Топ-10 продаваемых товаров ( 2 балла)
Напишите запрос, который выводит топ 10 самых продаваемых товаров каждый день
и проранжируйте их по дням и кол-ву проданных штук.
*/

WITH daily_sales AS (
    SELECT 
        sr.transaction_date::date AS transaction_date,
        p.product_name,
        SUM(sr.quantity) AS quantity_sold_per_day
    FROM coffe_shop.sales_reciepts sr
    	JOIN coffe_shop.product p 
    		ON sr.product_id = p.product_id
    GROUP BY 1,2
), ranked_sales AS (
    SELECT 
        transaction_date,
        product_name,
        quantity_sold_per_day,
        ROW_NUMBER() OVER (PARTITION BY transaction_date ORDER BY quantity_sold_per_day DESC) AS rating
    FROM daily_sales
)
SELECT 
    transaction_date,
    product_name,
    quantity_sold_per_day,
    rating
FROM ranked_sales
WHERE rating <= 10
ORDER BY 1,2;

/*
2. Кумулятивные продажи по дням ( 2 балла)
Напишите запрос, которые рассчитает кумулятивное число проданных продуктов по транзакциям.
*/

WITH cumulative_sales AS (
    SELECT 
        sr.transaction_date::date AS transaction_date,
        transaction_id,
        sr.quantity
    FROM coffe_shop.sales_reciepts sr
)
SELECT 
    transaction_date,
    quantity,
    SUM(quantity) OVER (ORDER BY transaction_date, transaction_id) AS com_quantity
FROM cumulative_sales
ORDER BY 1;

/*
3. Отклонение от максимальной продажи в дне ( 3 балла)
Напишите запрос, который по каждому менеджеру магазина и по каждому дню
выведет выручку кофешопа и отклонение этой выручки от максимальной выручки лучшего в этот день магазина. 
*/

WITH daily_revenue AS (
    SELECT 
        sr.transaction_date::date AS transaction_date,
        so.manager,
        SUM(sr.quantity * REPLACE(REPLACE(p.current_retail_price, '$', ''), ' ', '')::float) AS revenue
    FROM coffe_shop.sales_reciepts sr
    	JOIN coffe_shop.product p 
    		ON sr.product_id = p.product_id
    	JOIN coffe_shop.sales_outlet so 
    		ON sr.sales_outlet_id = so.sales_outlet_id
    GROUP BY 1,2
), max_daily_revenue AS (
    SELECT 
        transaction_date,
        MAX(revenue) AS max_revenue
    FROM daily_revenue
    GROUP BY transaction_date
), manager_names AS (
    SELECT 
        s.staff_id,
        CONCAT(s.last_name, ' ', s.first_name) AS manager_full_name
    FROM coffe_shop.staff s
)
SELECT 
    dr.transaction_date,
    mn.manager_full_name AS manager,
    dr.revenue,
    (m.max_revenue - dr.revenue) AS max_revenue_diff
FROM daily_revenue dr
	JOIN max_daily_revenue m 
		ON dr.transaction_date = m.transaction_date
	JOIN manager_names mn 
		ON dr.manager = mn.staff_id
ORDER BY 1,2;

/*
1. Среднее количество потерянных продуктов по кофешопам (1 балл)
Напишите запрос, который на 2019-04-07 выведет количество потерянных продуктов
в каждом отдельном кофешопе и среднее количество потерянных продуктов
по каждому из product_id среди всех кофешопов. 
*/

WITH waste_data AS (
    SELECT 
        pi.product_id,
        pi.sales_outlet_id,
        SUM(pi.waste) AS waste
    FROM coffe_shop.pastry_inventory pi
    WHERE pi.transaction_date = '04/07/2019'
    GROUP BY 1,2
)
SELECT 
    wd.product_id,
    wd.sales_outlet_id,
    wd.waste,
    AVG(wd.waste) OVER (PARTITION BY wd.product_id) AS avg_waste
FROM waste_data wd
ORDER BY 1,2;

/*
2. Сотрудники выше среднего * ( 3 балла)
Напишите запрос, который посчитает количество сотрудников по магазинам, у которых выручка от продаж выше, чем в средняя выручка на сотрудника по магазину.
В запросе должны использоваться оконные функции.
*/

WITH sales_data AS (
    SELECT 
        sr.sales_outlet_id,
        sr.staff_id,
        SUM(sr.quantity * REPLACE(REPLACE(p.current_retail_price, '$', ''), ' ', '')::float) AS sales_amount
    FROM coffe_shop.sales_reciepts sr
    	JOIN coffe_shop.product p 
    		ON sr.product_id = p.product_id
    GROUP BY 1,2
), avg_sales_per_staff AS (
    SELECT 
        sales_outlet_id,
        AVG(sales_amount) AS avg_sales_per_staff
    FROM sales_data
    GROUP BY 1
)
SELECT 
    sd.sales_outlet_id,
    COUNT(sd.staff_id) AS staff_qty
FROM sales_data sd
	JOIN avg_sales_per_staff aps 
		ON sd.sales_outlet_id = aps.sales_outlet_id
WHERE sd.sales_amount > aps.avg_sales_per_staff
GROUP BY 1
ORDER BY 1;

/*
1. Разница выручки между текущим и предыдущим днями
Напишите запрос, который рассчитывает разницу между выручкой в текущий и предыдущий день по данным таблицы coffe_shop.sales
*/
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
ORDER BY transaction_date;
 
/*
2. Скользящее среднее (2 балла)
Посчитайте 3-дневное скользящее среднее выручки каждого из кофешопов по дням.
*/
WITH daily_revenue AS (
    SELECT 
        sr.sales_outlet_id,
        sr.transaction_date::date AS transaction_date,
        SUM(sr.quantity * REPLACE(REPLACE(p.current_retail_price, '$', ''), ' ', '')::float) AS revenue
    FROM coffe_shop.sales_reciepts sr
    	JOIN coffe_shop.product p 
    		ON sr.product_id = p.product_id
    GROUP BY 1,2
)
SELECT 
    sales_outlet_id,
    transaction_date,
    revenue,
    AVG(revenue) OVER (
        PARTITION BY sales_outlet_id 
        ORDER BY transaction_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_days
FROM daily_revenue
ORDER BY 1,2;
    
/*
3. Изменения в процентах от среднего (3 балла)
Определить процентные изменения продаж между текущей продажей и средними продажами за последние 5 дней.
*/
WITH daily_sales AS (
    SELECT 
        sr.sales_outlet_id,
        sr.transaction_date::date AS transaction_date,
        SUM(sr.quantity) AS quantity
    FROM 
        coffe_shop.sales_reciepts sr
    GROUP BY 
        1,2
)
SELECT 
    sales_outlet_id,
    transaction_date,
    quantity,
	(quantity - AVG(quantity) OVER (
        PARTITION BY sales_outlet_id 
        ORDER BY transaction_date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )) / NULLIF(AVG(quantity) OVER (
        PARTITION BY sales_outlet_id 
        ORDER BY transaction_date 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 0) * 100 AS percent_change_sales
FROM daily_sales
ORDER BY 1,2;

/*
1. Фильтрация нужных сессий (1 балл)
Создайте временную таблицу, которая будет содержать данные о сессиях пользователей из таблицы mobile_game.sessions за апрель 2023 года. Временная таблица должна содержать:
user_id
session_start_time
После создания временной таблицы выполните SELECT для вывода данных о сессиях пользователей.
*/

CREATE TEMPORARY TABLE sessions_april AS (
SELECT
	user_id,
	session_start_time
FROM mobile_game.sessions
WHERE 1=1
	AND session_start_time >= '2023-04-01'
	AND session_start_time<'2023-05-01'
);

SELECT
	table_schema,
	table_name
FROM information_schema.tables
WHERE table_name = 'revenue_march_june'

SELECT * FROM pg_temp_95.sessions_april;

/*
2. Анализ поведения пользователей с использованием временных таблиц ( 2 балла)
Работая с данными из схемы mobile_game БД project, создайте 3 временные таблицы:
Первая временная таблица должна содержать информацию о пользователях и их общей выручке из таблицы transactions за период с марта 2023 года по июнь 2023 года.
Вторая временная таблица должна содержать информацию о количестве сессий пользователей из таблицы sessions за тот же период.
Объедините эти две временные таблицы в третьей с помощью JOIN по полю user_id, чтобы получить информацию о выручке и количестве сессий для каждого платящего пользователя за этот период.
Выведите пользователей, у которых общая выручка превышает 100, и количество сессий больше 5.
Напишите аналогичный запрос без использования временных таблиц и сравните скорость выполнения.
*/

CREATE TEMPORARY TABLE revenue_march_june AS (
    SELECT 
        user_id, 
        SUM(revenue * quantity) AS total_revenue
    FROM mobile_game.transactions
    WHERE 1=1 
    	AND event_date >= '2023-03-01' 
        AND event_date < '2023-07-01'
    GROUP BY 1
);

CREATE TEMPORARY TABLE sessions_march_june AS (
    SELECT 
        user_id, 
        COUNT(*) AS session_count
    FROM mobile_game.sessions
    WHERE 1=1 
        AND session_start_time >= '2023-03-01' 
        AND session_start_time < '2023-07-01'
    GROUP BY 1
);

CREATE TEMPORARY TABLE revenue_sessions_march_june AS (
    SELECT 
        r.user_id, 
        r.total_revenue, 
        s.session_count
    FROM pg_temp_9.revenue_march_june r
    	JOIN pg_temp_9.sessions_march_june s
    		ON r.user_id = s.user_id
);

SELECT 
    user_id, 
    total_revenue, 
    session_count
FROM pg_temp_9.revenue_sessions_march_june
WHERE 1=1
    and total_revenue > 100 
    AND session_count > 5;
    
SELECT 
    r.user_id, 
    r.total_revenue, 
    s.session_count
FROM 
    (SELECT 
        user_id, 
        SUM(revenue * quantity) AS total_revenue
     FROM mobile_game.transactions
     WHERE 1=1
        AND event_date >= '2023-03-01' 
        AND event_date < '2023-07-01'
     GROUP BY 1) r
	JOIN 
	    (SELECT 
	        user_id, 
	        COUNT(*) AS session_count
	     FROM 
	        mobile_game.sessions
	     WHERE 1=1
	        AND session_start_time >= '2023-03-01' 
	        AND session_start_time < '2023-07-01'
	     GROUP BY 1) s
		ON r.user_id = s.user_id
WHERE 1=1
    and r.total_revenue > 100 
    AND s.session_count > 5;
    
/*
3. Когортный анализ активных в марте игроков (4 балла)
Напишите код, который создаст временную таблицу, которая поможет провести когортный анализ для пользователей, которые были активны в марте 2023 года, с учетом их часовых поясов. Анализ должен показать, сколько пользователей возвращалось в приложение (имели сессии) в последующие месяцы.
Создайте временную таблицу, которая будет отображать когортный анализ по месяцам: сколько пользователей из когорты марта 2023 возвращалось в следующие месяцы (апрель, май и т.д.) до декабря 2023 года.
Учитывайте временные зоны пользователей при вычислении дат начала сессий.
С использование созданной временной таблицы напишите запрос, который выведет:
Месяц когорты (март, апрель и т.д.).
Количество пользователей, которые вернулись в этом месяце.
Процент возврата относительно числа пользователей, начавших взаимодействие в марте.
Работаем с БД project и схемой mobile_game
*/
   
CREATE TEMPORARY TABLE cohort_analysis AS (
    WITH march_users AS (
        SELECT 
            user_id, 
            session_start_time AT TIME ZONE user_info.timezone AS local_session_start_time
        FROM mobile_game.sessions 
        	LEFT JOIN mobile_game.user_info
        		USING(user_id)
        WHERE 1=1
            and session_start_time >= '2023-03-01' 
            AND session_start_time < '2023-04-01'
    ), subsequent_sessions AS (
        SELECT 
            user_id, 
            session_start_time AT TIME ZONE user_info.timezone AS local_session_start_time
        FROM mobile_game.sessions
            LEFT JOIN mobile_game.user_info
        		USING(user_id)
        WHERE 1=1
            and session_start_time >= '2023-04-01' 
            AND session_start_time < '2023-12-01'
    ), cohort_data AS (
        SELECT 
            mu.user_id,
            DATE_TRUNC('month', mu.local_session_start_time) AS cohort_month,
            DATE_TRUNC('month', ss.local_session_start_time) AS return_month
        FROM march_users mu
        	LEFT JOIN subsequent_sessions ss
        		ON mu.user_id = ss.user_id
    )
    SELECT 
        cohort_month,
        return_month,
        COUNT(DISTINCT user_id) AS returning_users
    FROM cohort_data
    GROUP BY 1,2
);

SELECT
	table_schema,
	table_name
FROM information_schema.tables
WHERE table_name = 'cohort_analysis'

SELECT 1 FROM pg_temp_38.cohort_analysis;

WITH total_users_per_cohort AS (
    SELECT 
        DATE_TRUNC('month', local_session_start_time) AS cohort_month,
        COUNT(DISTINCT user_id) AS total_users
    FROM 
        (SELECT 
            s.user_id, 
            s.session_start_time AT TIME ZONE u.timezone AS local_session_start_time
        FROM mobile_game.sessions s
        	JOIN mobile_game.user_info u
        		ON s.user_id = u.user_id
        WHERE 1=1
            and s.session_start_time >= '2023-03-01' 
            AND s.session_start_time < '2023-04-01') AS march_users
    	GROUP BY 1
)
SELECT 
    ca.cohort_month,
    ca.returning_users,
    ROUND(100.0 * ca.returning_users / tu.total_users, 2) AS return_percentage
FROM pg_temp_38.cohort_analysis ca
	JOIN total_users_per_cohort tu
		ON ca.cohort_month = tu.cohort_month
ORDER BY 1;