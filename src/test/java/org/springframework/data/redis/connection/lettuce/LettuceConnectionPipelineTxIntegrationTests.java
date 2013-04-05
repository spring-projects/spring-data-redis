package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;

/**
 * Integration test of {@link LettuceConnection} transactions within a pipeline
 *
 * @author Jennifer Hickey
 *
 */
public class LettuceConnectionPipelineTxIntegrationTests extends
		LettuceConnectionTransactionIntegrationTests {

	@Test(expected = RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test
	public void testDbSize() {
		connection.set("dbparam", "foo");
		assertNull(connection.dbSize());
		List<Object> results = getResults();
		assertEquals(1, results.size());
		assertTrue((Long) results.get(0) > 0);
	}

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	protected List<Object> getResults() {
		assertNull(connection.exec());
		List<Object> results = new ArrayList<Object>();
		List<Object> pipelinedResults = connection.closePipeline();
		// filter out the return value of exec
		for (Object result : pipelinedResults) {
			if (!"OK".equals(result) && !("QUEUED").equals(result)) {
				results.add(result);
			}
		}
		return results;
	}

}
