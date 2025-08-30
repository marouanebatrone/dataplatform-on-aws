
--creation table pointing to transformed_bi in s3
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_listings_bi (
    id STRING,
    name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    room_type STRING,
    price DOUBLE,
    minimum_nights DOUBLE,
    number_of_reviews DOUBLE,
    last_review STRING,
    reviews_per_month DOUBLE,
    calculated_host_listings_count DOUBLE,
    availability_365 DOUBLE,
    number_of_reviews_ltm DOUBLE,
    license STRING
)
PARTITIONED BY (city STRING)
STORED AS PARQUET
LOCATION 's3://airbnb-listings268/transformed_bi/';


MSCK REPAIR TABLE airbnb_listings_bi;



-- Some analysis queries:

-- Average price per room type per city
SELECT city, room_type, AVG(price) AS avg_price
FROM airbnb_listings_bi
GROUP BY city, room_type
ORDER BY city, avg_price DESC;

--Cities with highest average reviews per month
SELECT city, AVG(reviews_per_month) AS avg_reviews
FROM airbnb_listings_bi
GROUP BY city
ORDER BY avg_reviews DESC;

--Availability trends
SELECT city, AVG(availability_365) AS avg_availability
FROM airbnb_listings_bi
GROUP BY city
ORDER BY avg_availability DESC;

--Listings with extreme minimum nights
SELECT city, name, minimum_nights
FROM airbnb_listings_bi
WHERE minimum_nights > 365
ORDER BY minimum_nights DESC;