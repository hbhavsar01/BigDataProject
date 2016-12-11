-- use inc5000 database
use inc5000;

-- set the options for dynamic partition
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=100;
SET hive.exec.max.dynamic.partitions.pernode=100;

-- create an external table with dynamic partition
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_dp_allstates( num smallint,
source string, resultnumber smallint, id int, rank smallint, workers int,
company string, city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
PARTITIONED BY(state string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

