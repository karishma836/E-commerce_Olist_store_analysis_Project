show databases;

Create database if not exists Olist_E_Commerce;
#SELECT @@secure_file_priv;
use Olist_E_Commerce;


create table olist_order_items
(
order_id varchar (255),
product_id varchar (255),
seller_id varchar (255),
shipping_limit_date date,
price decimal (10,2),
freight_value decimal (10,2)
);

select *
from olist_order_items;

LOAD DATA INFILE 'olist_order_items.csv' into table olist_order_items
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

create table olist_order_reviews
(
review_id varchar (355),
order_id varchar (355),
review_score int,
review_creation_date date,
review_answer_timestamp date
);

select * from olist_order_reviews;
select count(review_id) from olist_order_reviews;

LOAD DATA INFILE 'olist_order_reviews_datasett.csv' into table olist_order_reviews
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

create table olist_customers
(
customer_id varchar (355),
customer_unique_id varchar (355),
customer_zip_code_prefix varchar (50),
customer_city varchar (255),
customer_state varchar (100)
);

select * from olist_customers;

LOAD DATA INFILE 'olist_customers.csv' into table olist_customers
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

create table olist_orders
(
order_id varchar (255),
customer_id varchar (255),
order_status varchar (200),
order_purchase_timestamp varchar (255) null,
order_delivered_carrier_date varchar (255) null,
order_delivered_customer_date varchar (255) null
);

select * from olist_orders;

LOAD DATA INFILE 'olist_orders_dataset.csv' into table olist_orders
FIELDS TERMINATED BY ','
enclosed by '"'
lines terminated by '\n'
IGNORE 1 LINES;

CREATE TABLE payment
(
order_id VARCHAR(255),
payment_type VARCHAR(200),
payment_installments INT,
payment_value DECIMAL (10,2)
);

LOAD DATA INFILE 'payment.csv' into table payment
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

alter table payment
rename to olist_order_payments;

CREATE TABLE olist_products
(
product_id VARCHAR (255),
product_category_name VARCHAR(255),
product_description_lenght varchar(4000)
);
drop table olist_products;
Select * from olist_products;
LOAD DATA INFILE 'olist_products.csv' into table olist_products
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

create table olist_sellers
(
seller_id varchar (255),
seller_zip_code_prefix varchar(5),
seller_city varchar (200),
seller_state varchar(2)
);

select * from olist_sellers;
drop table olist_sellers;
LOAD DATA INFILE 'olist_sellers.csv' into table olist_sellers
FIELDS TERMINATED BY ','
IGNORE 1 LINES;



#1. Weekday Vs Weekend (order_purchase_timestamp) Payment Statistics

create view KPI_1 AS
 select
   CASE
      WHEN dayofweek(order_purchase_timestamp) in (1,7) then 'Weekend' else 'Weekday'
      end as Day, 
      sum(round(payment_value)) as SUM_PAYMENT_VALUE
      from olist_orders o
      join olist_order_payments p
      on o.order_id = p.order_id
      group by day
      order by day;
      
 select * from KPI_1;
 drop view kpi_1; 
  
#2. Number of Orders with review score 5 and payment type as credit card.
  
 create view KPI_2 AS 
select count(*) as Total_Orders
from olist_order_reviews r
inner join olist_order_payments pmt
on r.order_id = pmt.order_id
where  review_score = 5
and payment_type = 'credit_card';
      
Select * from KPI_2;
drop view kpi_2; 

#3. Average number of days taken for order_delivered_customer_date for pet_shop

create view KPI_3 AS
select
round(avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp))) as Avg_delivery_Days
from olist_orders o
join olist_order_items it
on o.order_id = it.order_id
join olist_products p
on it.product_id = p.product_id
where product_category_name = 'pet_shop';


Select * from KPI_3;
drop view kpi_3;
#####2nd method for calculating kpi 3
create table KPI_3 AS
SELECT
    round(AVG(DATEDIFF(order_delivered_customer_date,order_purchase_timestamp)),0) AS avg_delivery_days
FROM
   olist_orders o
WHERE
    EXISTS (
        SELECT 1
        FROM olist_order_items i
        WHERE i.order_id = o.order_id
        AND EXISTS (
            SELECT 1
            FROM olist_products p
            WHERE p.product_id = i.product_id
            AND product_category_name = 'Pet_Shop'));


#4. Average price and payment values from customers of sao paulo city

create view KPI_4 AS
WITH A AS (
SELECT round(avg(price),0) as avg_price
from olist_order_items i
join olist_orders o on i.order_id = o.order_id
join olist_customers c on o.customer_id = c.customer_id
where customer_city = "sao paulo"
)
select
   (SELECT avg_price from A ) as Avg_city_price,
   round(avg(payment_value),0) as avg_city_payment
from olist_order_payments p
join olist_orders o on p.order_id = o.order_id
join olist_customers c on o.customer_id = c.customer_id
where customer_city = "sao paulo";

Select * from KPI_4;
drop view kpi_4;

#5. Relationship between shipping days (order_delivered_customer_date- order_purchase_timestamp) Vs review scores.

    create VIEW KPI_5 AS
    select
    r.review_score,
    round(avg(datediff(o.order_delivered_customer_date, o.order_purchase_timestamp))) as Avg_shipping_days
    from olist_orders o
    join olist_order_reviews r
    on r.order_id = o.order_id 
	group by r.review_score
	order by r.review_score;
    
Select * from KPI_5;
drop view kpi_5;

#6. Top 10 or bottom 10 products by price

create view KPI_6_TOP AS
select
 product_category_name , round(sum(price)) as Total_price
 from olist_order_items i
 join olist_products p
 on i.product_id = p.product_id
 group by product_category_name
 order by Total_price
 desc limit 10;

  create view KPI_6_BOTTOM AS
 select
 product_category_name , round(sum(price)) as Total_price
 from olist_order_items i
 join olist_products p
 on i.product_id = p.product_id
 group by product_category_name
 order by Total_price
 asc limit 10;
 
 SELECT * FROM KPI_6_TOP;
 select * FROM KPI_6_BOTTOM;
DROP VIEW KPI_6_TOP;
 DROP VIEW KPI_6_BOTTOM;
 
 #7.average price or freight value for sellers from a particualr state
 
 create view KPI_7 AS
 select
 s.seller_state , round(avg(price),2) as Avg_Price , round(avg(freight_value),2) as Avg_Freight
 FROM olist_sellers S
 JOIN olist_order_items i 
 on s.seller_id = i.seller_id
 group by s.seller_state
 order by Avg_Price,Avg_Freight;
 
Select * from KPI_7;
drop view kpi_7;


#1. Weekday Vs Weekend (order_purchase_timestamp) Payment Statistics
select * FROM KPI_1;
  
#2. Number of Orders with review score 5 and payment type as credit card.
select * FROM KPI_2;

#3. Average number of days taken for order_delivered_customer_date for pet_shop
select * FROM KPI_3;

#4. Average price and payment values from customers of sao paulo city
select * FROM KPI_4;

#5. Relationship between shipping days (order_delivered_customer_date- order_purchase_timestamp) Vs review scores.
select * FROM KPI_5;

#7. Top 10 or bottom 10 products by price
select * FROM KPI_6_top;
select * FROM KPI_6_bottom;

#6.average price or freight value for sellers from a particualr state
select * FROM KPI_7;

alter TABLE olist_orders
modify column order_purchase_timestamp DATE;
      

