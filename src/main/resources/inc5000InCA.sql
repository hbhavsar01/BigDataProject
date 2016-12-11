-- use inc5000 database
use inc5000;

-- create a table store total company counts from CA
CREATE TABLE IF NOT EXISTS companylist_ca_ny(
state string,
company_count BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
