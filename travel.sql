--Create the user defined enum type season
DROP TYPE IF EXISTS season;
CREATE TYPE season AS ENUM ('Spring', 'Summer', 'Autumn', 'Winter');

-- Create the customer table
CREATE TABLE IF NOT EXISTS customer (
   customer_id SERIAL PRIMARY KEY,
   first_name VARCHAR(30) NOT NULL,
   last_name VARCHAR(30) NOT NULL,
   email VARCHAR(40) UNIQUE NOT NULL,
   phone VARCHAR(30) NOT NULL
);

-- Create the destination table
CREATE TABLE IF NOT EXISTS destination (
   destination_id SERIAL PRIMARY KEY,
   destination VARCHAR(50) NOT NULL,
   country VARCHAR(30) NOT NULL,
   popular_season season NOT NULL
);

-- Create the booking table
CREATE TABLE IF NOT EXISTS booking (
    booking_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    destination_id INT NOT NULL,
    booking_date DATE NOT NULL,
    cost_per_passenger NUMERIC(10, 2) NOT NULL,
    number_of_passengers INT NOT NULL,
    total_booking_value NUMERIC(10, 2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
    FOREIGN KEY (destination_id) REFERENCES destination (destination_id)
);