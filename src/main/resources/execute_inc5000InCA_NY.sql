-- use inc5000 database
use inc5000;

-- create a table to store NY & CA company counts
INSERT INTO TABLE companylist_ca_ny
SELECT companylist.state as State, count(companylist.id) as Count
FROM companylist
WHERE companylist.state = 'CA' OR companylist.state = 'NY'
GROUP BY companylist.state;

