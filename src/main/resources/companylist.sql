-- create an inc5000 database
CREATE DATABASE IF NOT EXISTS inc5000;

-- use inc5000 database
use inc5000;

-- create an external table
CREATE EXTERNAL TABLE IF NOT EXISTS companylist( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, state string,
city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- load data from inc5000.csv
LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/BigDataProject/src/main/resources/inc5000.csv'
OVERWRITE INTO TABLE companylist;
