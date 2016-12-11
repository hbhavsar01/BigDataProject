-- use inc5000 database
use inc5000;

-- set the cost based optimization properties
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- create an external table with ORC format
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_cbo_allstates ( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, state string,
city string, metro string, growth double,
revenue double, industry string, yrs_on_listex int
)
STORED AS ORCFILE;

