package org.springframework.data.redis.connection.jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.test.annotation.IfProfileValue;

public class JedisConnectionPipelineTxIntegrationTests extends JedisConnectionTransactionIntegrationTests {

	@Ignore("Jedis issue: Pipeline tries to return String instead of List<String>")
	@Test
	public void testGetConfig() {}

	@Test(expected = RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("set", "foo");
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
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
		assertEquals(1, pipelined.size());
		List<Object> txResults = (List<Object>) pipelined.get(0);
		// Return exec results and this test should behave exactly like its superclass
		return txResults;
	}
}
