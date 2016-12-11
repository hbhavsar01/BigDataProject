package com.mycompany.test;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.mycompany.app.HiveScriptParse;

/**
 * The HiveScriptParseTest has various tests for parsing SQL scripts in
 * statements and compares them to expected results
 *
 */
public class HiveScriptParseTest {

	@Test
	public void testCompanylistScript() {
		String scriptFile = "src/main/resources/companylist.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(4, statements.size());

		String line0 = "CREATE DATABASE IF NOT EXISTS inc5000";
		assertEquals(line0, statements.get(0));

		String line1 = "use inc5000";
		assertEquals(line1, statements.get(1));

		String line2 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist( num smallint, source string, resultnumber smallint, id int, "
				+ "rank smallint, workers int, company string, state string, city string, "
				+ "metro string, growth double, revenue double, industry string, yrs_on_list int ) "
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
		assertEquals(line2, statements.get(2));
		String line3 = "LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/BigDataProject/src/main/resources/inc5000.csv' "
				+ "OVERWRITE INTO TABLE companylist";
		assertEquals(line3, statements.get(3));
	}

	@Test
	public void testCompanylistDyamicPartitionScript() {
		String scriptFile = "src/main/resources/companylist_dp.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(7, statements.size());

		String line0 = "use inc5000";
		assertEquals(line0, statements.get(0));

		String line1 = "SET hive.exec.dynamic.partition = true";
		assertEquals(line1, statements.get(1));

		String line2 = "SET hive.exec.dynamic.partition.mode = nonstrict";
		assertEquals(line2, statements.get(2));

		String line3 = "SET hive.exec.max.dynamic.partitions=100";
		assertEquals(line3, statements.get(3));

		String line4 = "SET hive.exec.max.dynamic.partitions.pernode=100";
		assertEquals(line4, statements.get(4));

		String line5 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist_dp( num smallint, source string, "
				+ "resultnumber smallint, id int, rank smallint, workers int, company string, city string, "
				+ "metro string, growth double, revenue double, industry string, yrs_on_list int ) "
				+ "PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
		assertEquals(line5, statements.get(5));
		String line6 = "INSERT OVERWRITE TABLE companylist_dp PARTITION (state)  SELECT num, source, resultnumber, "
				+ "id, rank, workers, company, city, metro, growth, revenue, industry, yrs_on_list, state FROM companylist";
		assertEquals(line6, statements.get(6));
	}

	@Test
	public void testCompanylistBucketingScript() {
		String scriptFile = "src/main/resources/companylist_buck.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(4, statements.size());

		String line0 = "use inc5000";
		assertEquals(line0, statements.get(0));

		String line1 = "SET hive.enforce.bucketing = true";
		assertEquals(line1, statements.get(1));

		String line2 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist_buck( num smallint, source string, "
				+ "resultnumber smallint, id int, rank smallint, workers int, company string, state string, "
				+ "city string, metro string, growth double, revenue double, industry string, yrs_on_list int ) "
				+ "CLUSTERED BY (resultnumber) INTO 32 BUCKETS  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
				+ "STORED AS TEXTFILE";
		assertEquals(line2, statements.get(2));
		String line3 = "INSERT OVERWRITE TABLE companylist_buck SELECT num, source, resultnumber, id, rank, "
				+ "workers, company, state, city, metro, growth, revenue, industry, yrs_on_list FROM companylist";
		assertEquals(line3, statements.get(3));
	}

	@Test
	public void testCompanylistCBOScript() {
		String scriptFile = "src/main/resources/companylist_cbo.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(7, statements.size());

		String line0 = "use inc5000";
		assertEquals(line0, statements.get(0));

		String line1 = "SET hive.cbo.enable=true";
		assertEquals(line1, statements.get(1));

		String line2 = "SET hive.compute.query.using.stats=true";
		assertEquals(line2, statements.get(2));

		String line3 = "SET hive.stats.fetch.column.stats=true";
		assertEquals(line3, statements.get(3));

		String line4 = "SET hive.stats.fetch.partition.stats=true";
		assertEquals(line4, statements.get(4));

		String line5 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist_cbo ( num smallint, source string, "
				+ "resultnumber smallint, id int, rank smallint, workers int, company string, state string, "
				+ "city string, metro string, growth double, revenue double, industry string, yrs_on_listex int ) "
				+ "STORED AS ORCFILE";
		assertEquals(line5, statements.get(5));
		String line6 = "INSERT OVERWRITE TABLE companylist_cbo  SELECT num, source, resultnumber, id, rank, workers, "
				+ "company, state, city, metro, growth, revenue, industry, yrs_on_list FROM companylist";
		assertEquals(line6, statements.get(6));
	}

	@Test
	public void testInc5000Script() {
		String scriptFile = "src/main/resources/inc5000.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(2, statements.size());
		assertEquals("use inc5000", statements.get(0));

		String line1 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist( num smallint, source string, resultnumber smallint, id int, "
				+ "rank smallint, workers int, company string, state string, city string, "
				+ "metro string, growth double, revenue double, industry string, yrs_on_list int ) "
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";
		assertEquals(line1, statements.get(1));
	}

	@Test
	public void testInc5000InCAScript() {
		String scriptFile = "src/main/resources/inc5000InCA.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(2, statements.size());
		assertEquals("use inc5000", statements.get(0));
		String line1 = "CREATE TABLE IF NOT EXISTS companylist_ca_ny( state string, company_count BIGINT ) "
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";
		assertEquals(line1, statements.get(1));
	}

	@Test
	public void testExecuteInc5000InCA_NYScript() {
		String scriptFile = "src/main/resources/execute_inc5000InCA_NY.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(2, statements.size());
		assertEquals("use inc5000", statements.get(0));
		String line1 = "INSERT INTO TABLE companylist_ca_ny SELECT companylist.state as State, count(companylist.id) "
				+ "as Count FROM companylist WHERE companylist.state = 'CA' OR companylist.state = 'NY' GROUP BY companylist.state";
		assertEquals(line1, statements.get(1));
	}

	@Test
	public void testCompanylistSPAllStatesScript() {
		String scriptFile = "src/main/resources/companylist_sp_allstates.sql";
		Map<String, String> params = null;
		List<String> excludes = null;
		HiveScriptParse hiveScript = new HiveScriptParse(scriptFile, params,
				excludes);
		List<String> statements = hiveScript.getStatements();
		assertEquals(2, statements.size());
		String line0 = "use inc5000";
		assertEquals(line0, statements.get(0));
		String line1 = "CREATE EXTERNAL TABLE IF NOT EXISTS companylist_sp_allstates( num smallint, source string, resultnumber smallint,"
				+ " id int, rank smallint, workers int, company string, city string, metro string, growth double, revenue double, "
				+ "industry string, yrs_on_list int ) PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
		assertEquals(line1, statements.get(1));
	}

}
