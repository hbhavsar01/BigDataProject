-- use inc5000 database
use inc5000;

-- create an external table with static partition
CREATE EXTERNAL TABLE IF NOT EXISTS companylist_sp( num smallint,
source string,
resultnumber smallint, id int,
rank smallint, workers int,
company string, city string, metro string, growth double,
revenue double, industry string, yrs_on_list int
)
PARTITIONED BY(state string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- insert data for state of california from companylist 
INSERT OVERWRITE TABLE companylist_sp PARTITION (state='CA') 
SELECT num, source, resultnumber, id, rank, workers, company, city, metro, growth, revenue, 
industry, yrs_on_list FROM companylist where state='CA';


