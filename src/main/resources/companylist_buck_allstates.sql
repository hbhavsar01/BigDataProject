-- use inc5000 database
use inc5000;

-- set the backeting option
SET hive.enforce.bucketing = true;

-- create an external table with bucketing (32 Buckets)
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_buck_allstates( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, state string, city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
CLUSTERED BY (resultnumber) INTO 32 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

