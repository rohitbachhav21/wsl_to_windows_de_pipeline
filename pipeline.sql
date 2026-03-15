create database de_pipeline;
use de_pipeline;
CREATE TABLE pizza_types (
    pizza_type_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    ingredients TEXT
);

CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    time TIME
);

CREATE TABLE order_details (
    order_details_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    pizza_id VARCHAR(50),
    quantity INT
);

CREATE TABLE pizzas (
    pizza_id VARCHAR(50) PRIMARY KEY,
    pizza_type_id VARCHAR(50),
    size VARCHAR(10),
    price DECIMAL(5,2)
);


-- converting orders table

alter table orders add primary key (order_id);
alter table orders modify order_id int not null auto_increment;

-- converting order_details
ALTER TABLE order_details
MODIFY order_details_id INT NOT NULL;

ALTER TABLE order_details
ADD PRIMARY KEY (order_details_id);

ALTER TABLE order_details
MODIFY order_details_id INT AUTO_INCREMENT;

select * from orders order by order_id desc;