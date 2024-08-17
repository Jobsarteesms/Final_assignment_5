CREATE TABLE weather_data (
    city STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    temperature DOUBLE,
    humidity INT,
    weather_description STRING,
    wind_speed DOUBLE,
    wind_direction INT,
    timestamp STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;