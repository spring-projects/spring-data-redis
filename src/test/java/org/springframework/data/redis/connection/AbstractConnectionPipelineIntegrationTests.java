/*
 * Copyright 2011-2018 the original author or authors.
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

package org.springframework.data.redis.connection;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Base test class for integration tests that execute each operation of a Connection while a pipeline is open, verifying
 * that the operations return null and the proper values are returned when closing the pipeline.
 * <p>
 * Pipelined results are generally native to the provider and not transformed by our {@link RedisConnection}, so this
 * test overrides {@link AbstractConnectionIntegrationTests} when result types are different
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
abstract public class AbstractConnectionPipelineIntegrationTests extends AbstractConnectionIntegrationTests {

	@Ignore
	public void testNullKey() throws Exception {}

	@Ignore
	public void testNullValue() throws Exception {}

	@Ignore
	public void testHashNullKey() throws Exception {}

	@Ignore
	public void testHashNullValue() throws Exception {}

	@Ignore("Pub/Sub not supported while pipelining")
	public void testPubSubWithNamedChannels() throws Exception {}

	@Ignore("Pub/Sub not supported while pipelining")
	public void testPubSubWithPatterns() throws Exception {}

	@Test(expected = RedisPipelineException.class)
	public void testExecWithoutMulti() {
		super.testExecWithoutMulti();
	}

	@Test(expected = RedisPipelineException.class)
	public void testErrorInTx() {
		super.testErrorInTx();
	}

	@Test(expected = RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		super.exceptionExecuteNative();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaNotFound() {
		super.testEvalShaNotFound();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnSingleError() {
		super.testEvalReturnSingleError();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalArrayScriptError() {
		super.testEvalArrayScriptError();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayError() {
		super.testEvalShaArrayError();
	}

	@Test
	public void testOpenPipelineTwice() {
		connection.openPipeline();
		// ensure things still proceed normally with an extra openPipeline
		testGetSet();
	}

	@Test
	public void testClosePipelineNotOpen() {
		getResults();
		List<Object> results = connection.closePipeline();
		assertTrue(results.isEmpty());
	}

	@Test // DATAREDIS-417
	@Ignore
	@Override
	public void scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection() {
		super.scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection();
	}

	protected void initConnection() {
		connection.openPipeline();
	}

	protected void verifyResults(List<Object> expected) {
		List<Object> expectedPipeline = new ArrayList<>();
		for (int i = 0; i < actual.size(); i++) {
			expectedPipeline.add(null);
		}
		assertEquals(expectedPipeline, actual);
		List<Object> results = getResults();
		assertEquals(expected, results);
	}

	protected List<Object> getResults() {

		try {
			// we give redis some time to keep up
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return connection.closePipeline();
	}
}
