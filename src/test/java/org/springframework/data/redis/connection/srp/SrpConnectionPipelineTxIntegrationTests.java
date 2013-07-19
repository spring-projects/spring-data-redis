/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.srp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link SrpConnection} transactions within a pipeline
 *
 * @author Jennifer Hickey
 *
 */
public class SrpConnectionPipelineTxIntegrationTests extends SrpConnectionTransactionIntegrationTests {


	@Test
	public void testUsePipelineAfterTxExec() {
		connection.set("foo", "bar");
		assertNull(connection.exec());
		assertNull(connection.get("foo"));
		List<Object> results = connection.closePipeline();
		assertEquals(Arrays.asList(new Object[] {Collections.singletonList("OK"), "bar"}), results);
		assertEquals("bar", connection.get("foo"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExec2TransactionsInPipeline() {
		connection.set("foo", "bar");
		assertNull(connection.get("foo"));
		assertNull(connection.exec());
		connection.multi();
		connection.set("foo", "baz");
		assertNull(connection.get("foo"));
		assertNull(connection.exec());
		List<Object> results = connection.closePipeline();
		assertEquals(2, results.size());
		// First element of each list is "OK"
		assertEquals("bar", new String((byte[])((List<Object>)results.get(0)).get(1)));
		assertEquals("baz", new String((byte[])((List<Object>)results.get(1)).get(1)));
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		super.testEvalShaNotFound();
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		super.testEvalReturnSingleError();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitOpNotMultipleSources() {
		super.testBitOpNotMultipleSources();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreBadData() {
		super.testRestoreBadData();
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
