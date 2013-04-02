package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertNull;

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

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	protected List<Object> getResults() {
		assertNull(connection.exec());
		List<Object> results = new ArrayList<Object>();
		List<Object> pipelinedResults = connection.closePipeline();
		for (Object result : pipelinedResults) {
			if (!"OK".equals(result) && !("QUEUED").equals(result)) {
				results.add(result);
			}
		}
		return results;
	}

}
