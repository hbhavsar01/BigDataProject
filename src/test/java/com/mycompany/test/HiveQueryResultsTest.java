package com.mycompany.test;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

/**
 * The HiveQueryResultsTest has various tests to check the results from
 * different hive queries and scripts  
 *
 */
@RunWith(StandaloneHiveRunner.class)
public class HiveQueryResultsTest extends TestCase {
	private String database = "inc5000";

	@HiveSQL(files = {})
	private HiveShell hiveShell;

	@HiveRunnerSetup
	public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
		{
			setHiveExecutionEngine("mr");
		}
	};

	@Before
	public void Setup() throws FileNotFoundException {

		hiveShell.execute("CREATE DATABASE IF NOT EXISTS " + database + ";");
		hiveShell.execute(getResourceFile("inc5000.sql"));
		hiveShell.execute(getResourceFile("inc5000InCA.sql"));
		hiveShell.execute(getResourceFile("companylist_dp_allstates.sql"));
		hiveShell.execute(getResourceFile("companylist_cbo_allstates.sql"));
		insertInto("companylist", "inc5000.csv");
	}

	@Test
	public void testInc5000TotalCompanyCount() throws FileNotFoundException {

		HashSet<String> expectedResultSet = Sets.newHashSet("5000");
		HashSet<String> actualResultSet = Sets.newHashSet(hiveShell
				.executeQuery("select count (*) from companylist"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000InCA_NYSQL() throws FileNotFoundException {

		hiveShell.execute(getResourceFile("execute_inc5000InCA_NY.sql"));

		List<Object[]> actualResultSet = hiveShell
				.executeStatement("select * from companylist_ca_ny order by State");

		List<Object[]> expectedResultSet = Arrays.asList(new Object[] { "CA",
				694L }, new Object[] { "NY", 335L });

		Assert.assertArrayEquals(expectedResultSet.toArray(),
				actualResultSet.toArray());
		Assert.assertEquals(expectedResultSet.size(), actualResultSet.size());
	}

	@Test
	public void testInc5000DynamicPartitionNYSQL() throws FileNotFoundException {

		insertInto("companylist_dp_allstates", "inc5000_NY.csv");

		List<Object[]> actualResultSet = hiveShell
				.executeStatement("select * from companylist_dp_allstates where state='NY'");

		Assert.assertEquals(335, actualResultSet.size());
	}

	@Test
	public void testInc5000DynamicPartitionCASQL() throws FileNotFoundException {

		insertInto("companylist_dp_allstates", "inc5000_CA.csv");

		List<Object[]> actualResultSet = hiveShell
				.executeStatement("select * from companylist_dp_allstates where state='CA'");

		Assert.assertEquals(694, actualResultSet.size());
	}

	@Test
	public void testInc5000CostBasedOptimizationSQL()
			throws FileNotFoundException {

		insertInto("companylist_cbo_allstates", "inc5000.csv");

		List<Object[]> actualResultSet = hiveShell
				.executeStatement("select * from companylist_cbo_allstates where state='TX'");

		Assert.assertEquals(404, actualResultSet.size());
	}

	@Test
	public void testInc5000TotalWorkersInWashingtonDC()
			throws FileNotFoundException {

		HashSet<String> expectedResultSet = Sets.newHashSet("3547");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select sum (workers) from companylist where state='DC' and city='Washington'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000WorkersMoreThan10000() {

		HashSet<String> expectedResultSet = Sets.newHashSet("9");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select count (*) from companylist where workers>=10000"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000AvgRevenueInMA() {

		HashSet<String> expectedResultSet = Sets
				.newHashSet("4.4338366672413796E7");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select avg (revenue) from companylist where state='MA'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000InSeattleWA() {

		HashSet<String> expectedResultSet = Sets.newHashSet("43");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select count (*) from companylist where state='WA' and city='Seattle'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000AvgGrowthInCA() {

		HashSet<String> expectedResultSet = Sets
				.newHashSet("876.1990778097971");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select avg (growth) from companylist where state='CA'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000MaxGrowthInTX() {

		HashSet<String> expectedResultSet = Sets.newHashSet("10522.64");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select max (growth) from companylist where state='TX'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000MinGrowthInNY() {

		HashSet<String> expectedResultSet = Sets.newHashSet("42.63");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select min (growth) from companylist where state='NY'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000SumGrowthInIL() {

		HashSet<String> expectedResultSet = Sets
				.newHashSet("88305.85999999997");
		HashSet<String> actualResultSet = Sets
				.newHashSet(hiveShell
						.executeQuery("select sum (growth) from companylist where state='IL'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	@Test
	public void testInc5000CompaniesInAK() {

		HashSet<String> expectedResultSet = Sets
				.newHashSet("4220	Inc5000 Company List	4220	26261	4220	38	"
						+ "Spawn Ideas	AK	Anchorage	Anchorage AK	67.57	1.2316709E7	Advertising & Marketing	1");
		HashSet<String> actualResultSet = Sets.newHashSet(hiveShell
				.executeQuery("select * from companylist where state='AK'"));

		Assert.assertEquals(1, actualResultSet.size());
		Assert.assertEquals(expectedResultSet, actualResultSet);
	}

	/**
	 * This method inserts the data into table
	 * 
	 * @param table
	 * @param resourcePath
	 */
	private void insertInto(String table, String resourcePath) {
		String delimeter = ",";
		String nullValue = "\\n";

		hiveShell
				.insertInto(database, table)
				.withAllColumns()
				.addRowsFromDelimited(
						new File(getClass().getClassLoader()
								.getResource(resourcePath).getPath()),
						delimeter, nullValue).commit();
	}

	/**
	 * This method gets the file from resource path
	 * 
	 * @param resourcePath
	 * @return File
	 * @throws FileNotFoundException
	 */
	private File getResourceFile(String resourcePath)
			throws FileNotFoundException {
		URL url = this.getClass().getClassLoader().getResource(resourcePath);
		if (null == url) {
			throw new FileNotFoundException(resourcePath);
		}
		return new File(url.getPath());
	}
}
