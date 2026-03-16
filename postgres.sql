select * from pizza_orders_stream;

select count(*) from pizza_orders_stream;

CREATE TABLE pizza_orders_stage (
    order_id INT,
    order_date DATE,
    order_time TIME
);

-- run these 2 queries after every batch


INSERT INTO pizza_orders_stream
SELECT DISTINCT ON (order_id)
    order_id,
    order_date,
    order_time
FROM pizza_orders_stage
ORDER BY order_id
ON CONFLICT (order_id)
DO UPDATE SET
    order_date = EXCLUDED.order_date,
    order_time = EXCLUDED.order_time;


truncate pizza_orders_stage;