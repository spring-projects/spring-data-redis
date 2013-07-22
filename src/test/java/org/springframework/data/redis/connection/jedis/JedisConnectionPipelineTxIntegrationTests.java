package org.springframework.data.redis.connection.jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;

public class JedisConnectionPipelineTxIntegrationTests extends JedisConnectionTransactionIntegrationTests {

	@Ignore("Jedis issue: Pipeline tries to return String instead of List<String>")
	@Test
	public void testGetConfig() {
	}

	@Ignore("https://github.com/xetorthio/jedis/pull/389 Pipeline tries to return List<String> instead of Long on sort")
	public void testSortStore() {
	}

	@Ignore("Jedis issue: Pipeline tries to return Long instead of List<String> on sort with no params")
	public void testSortNullParams() {
	}

	@Ignore("https://github.com/xetorthio/jedis/pull/389 Pipeline tries to return Long instead of List<String> on sort with no params")
	public void testSortStoreNullParams() {
	}

	@Test
	public void testEcho() {
		actual.add(connection.echo("Hello World"));
		verifyResults(Arrays.asList(new Object[] { "Hello World" }));
	}

	@Test(expected=RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("set", "foo");
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test
	public void testLastSave() {
		connection.lastSave();
		List<Object> results = getResults();
		assertNotNull(results.get(0));
	}

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	@SuppressWarnings("unchecked")
	protected List<Object> getResults() {
		assertNull(connection.exec());
		List<Object> pipelined = connection.closePipeline();
		// We expect only the results of exec to be in the closed pipeline
		assertEquals(1,pipelined.size());
		List<Object> txResults = (List<Object>)pipelined.get(0);
		// Return exec results and this test should behave exactly like its superclass
		return convertResults(txResults);
	}

	@SuppressWarnings("unchecked")
	protected List<Object> getResultsNoConversion() {
		assertNull(connection.exec());
		List<Object> pipelined = connection.closePipeline();
		// We expect only the results of exec to be in the closed pipeline
		assertEquals(1,pipelined.size());
		List<Object> txResults = (List<Object>)pipelined.get(0);
		// Return exec results and this test should behave exactly like its superclass
		return txResults;
	}

}
