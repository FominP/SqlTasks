select count(*) from coffe_shop.product;

select unit_price from coffe_shop.sales where transaction_id = 110;
select 
	count(distinct transaction_id)
from coffe_shop.sales 
where 1=1
	and quantity>3;
	
select 
	count(*)
from coffe_shop.sales 
where 1=1
	and product_category = 'Coffee'
	and store_address = '100 Church Street';
	
select 
	beverage_goal + food_goal
from coffe_shop.sales_targets 
where 1=1
	and sales_outlet_id = 8;

/*
1) Раздел 2. Извлечение, фильтрация и преобразование данных:  ПЗ "Попарная сортировка"
2) Раздел 3. Агрегация: ПЗ DataQuality (будет самым последним в теме)
3) Раздел 4. Объединение таблиц и подзапросы: задачи с 3 по 7 
(Собираем витрину, Используем витрину, Доля покупающих, RFM, Metric - value)
*/

--1) Раздел 2. Извлечение, фильтрация и преобразование данных:  ПЗ "Попарная сортировка"
select
	customer_id
	, case when mod(customer_id, 2) = 0 then customer_id - 1
		else customer_id + 1
	end as customer_id_new
	, customer_name
from coffe_shop.customer
where 1=1
	and customer_id <= 100
order by customer_id;

--2) Раздел 3. Агрегация: ПЗ DataQuality (будет самым последним в теме)
select * from coffe_shop.customer_corrupted limit 5;

select 
	customer_name
	, case when regexp_like(customer_name, '^[A-Z][a-z]*\s[A-Z][a-z]*$')
	then 1 else 0 end as name_check
from coffe_shop.customer_corrupted;

select 
	email
	, case when regexp_like(email, '^[^\s@]+@[^\s@A-Z]+\.[a-z]{2,3}$')
	then 1 else 0 end as email_check
from coffe_shop.customer_corrupted;

select 
	birth_year
	, case when extract(year from current_date) - birth_year <= 100
	then 1 else 0 end as year_check
from coffe_shop.customer_corrupted;

select distinct gender from coffe_shop.customer_corrupted;

with raw_table as (
	select 
		customer_id
		, case 
			when (customer_name is not null
				and email is not null
				and gender is not null
				and birthdate is not null)
			then 1 else 0 end as completeness
		, case 
			--customer_name состоит из двух слов, каждое начинается с большой буквы и состоит только из латиницы
			when regexp_like(customer_name, '^[A-Z][a-z]*\s[A-Z][a-z]*$')
				--email содержит @ и оканчивается на домен верхнего уровня (длиной 2-3 символа после точки), доменная часть не содержит больших букв
				and regexp_like(email, '^[^\s@]+@[^\s@A-Z]+\.[a-z]{2,3}$')
				--проверяем возраст, используем birthdate, так как заполненность birthdate и birth_year не совпадает
				and extract(year from current_date) - CAST(SUBSTRING(birthdate, 1, 4) AS INTEGER) <= 100
			then 1 else 0 end as conformity
	from coffe_shop.customer_corrupted
)
select
	--по логике условия conformity нельзя проверить полностью без выполнения всех условий completeness, поэтому дана доля строк с completeness и conformity и отдельно с completeness
	round(sum(completeness) / COUNT(customer_id)::decimal, 2) as completeness
	, round(sum(conformity) / sum(completeness)::decimal, 2) as conformity
	, round(sum(conformity) / COUNT(customer_id)::decimal, 2) as conformity_and_completeness
from raw_table;

--3) Раздел 4. Объединение таблиц и подзапросы: задачи с 3 по 7 
--(Собираем витрину, Используем витрину, Доля покупающих, RFM, Metric - value)
with raw_table as (
	select
		customer_id
		, max(transaction_date) as last_purchase
		, count(distinct transaction_id) as num_purchases
		, sum(unit_price) * sum(quantity) as revenue
	from coffe_shop.sales
	group by customer_id
)
select
	case 
		when current_date - last_purchase > 15 then 1
		when current_date - last_purchase < 7 then 3
		else 2
	end as recency
	, case 
		when num_purchases < 10 then 1
		when num_purchases >= 23 then 3
		else 2
	end as frequency
	, case 
		when revenue < 50 then 1
		when num_purchases >= 110 then 3
		else 2
	end as monetary
	, sum(revenue) as revenue
from raw_table
group by 1,2,3;


/*
Сегменты, начинающиеся на 2 и 3 отсутствуют, так как максимальная дата в таблице - 2019-04-29

Либо нужны более актуальные данные,
либо нужно взять дату, от которой мы считаем сегменты (если мы хотим оценить ситуацию в прошлом),
либо покупатели забросили нашу кофейню
*/
select * from coffe_shop.sales limit 5;

--Сегмент 122 нужно попытаться реактивировать, чтобы пользователи снова зашли в кофейню, так как они активно делали покупки