package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link LettuceConnection} transactions within a pipeline
 * 
 * @author Jennifer Hickey
 */
public class LettuceConnectionPipelineTxIntegrationTests extends LettuceConnectionTransactionIntegrationTests {

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		super.testEvalShaNotFound();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		super.testEvalReturnSingleError();
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

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalArrayScriptError() {
		super.testEvalArrayScriptError();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaArrayError() {
		super.testEvalShaArrayError();
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
