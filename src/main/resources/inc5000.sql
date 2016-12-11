-- use inc5000 database 
use inc5000;

-- create an external table to store all inc5000 companies
CREATE EXTERNAL TABLE IF NOT EXISTS companylist( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, state string,
city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
