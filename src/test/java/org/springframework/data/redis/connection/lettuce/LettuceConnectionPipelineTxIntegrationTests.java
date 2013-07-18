package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

/**
 * Integration test of {@link LettuceConnection} transactions within a pipeline
 *
 * @author Jennifer Hickey
 *
 */
public class LettuceConnectionPipelineTxIntegrationTests extends
		LettuceConnectionTransactionIntegrationTests {

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
