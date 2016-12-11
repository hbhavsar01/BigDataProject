-- use inc5000 database
use inc5000;

-- create an external table with static partition
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_sp_allstates( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
PARTITIONED BY(state string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

