-- use inc5000 database
use inc5000;

-- set the backeting option
SET hive.enforce.bucketing = true;

-- create an external table with bucketing (32 Buckets)
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_buck( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, state string, city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
CLUSTERED BY (resultnumber) INTO 32 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- insert data from companylist in 32 buckets  
INSERT OVERWRITE TABLE companylist_buck
SELECT num, source, resultnumber, id, rank, workers, company, state, city, metro, growth, revenue, industry, yrs_on_list FROM companylist;

