/*
 * Copyright 2011-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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

	@Override
	@Disabled
	public void testNullKey() {}

	@Override
	@Disabled
	public void testNullValue() {}

	@Override
	@Disabled
	public void testHashNullKey() {}

	@Override
	@Disabled
	public void testHashNullValue() {}

	@Override
	@Disabled("Pub/Sub not supported while pipelining")
	public void testPubSubWithNamedChannels() throws Exception {}

	@Override
	@Disabled("Pub/Sub not supported while pipelining")
	public void testPubSubWithPatterns() throws Exception {}

	@Override
	@Test
	public void testExecWithoutMulti() {
		connection.exec();
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	public void testErrorInTx() {

		connection.multi();
		connection.set("foo", "bar");
		// Try to do a list op on a value
		connection.lPop("foo");
		connection.exec();
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	public void exceptionExecuteNative() {
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(() -> {
			connection.execute("set", "foo");
			getResults();
		});
	}

	@Override
	@Test
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	public void testRestoreBadData() {
		connection.restore("testing".getBytes(), 0, "foo".getBytes());
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	public void testRestoreExistingKey() {
		actual.add(connection.set("testing", "12"));
		actual.add(connection.dump("testing".getBytes()));
		List<Object> results = getResults();
		initConnection();
		connection.restore("testing".getBytes(), 0, (byte[]) results.get(1));
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
	}

	@Override
	@Test
	@Disabled
	public void testEvalArrayScriptError() {}

	@Override
	@Test
	public void testEvalShaArrayError() {
		connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1");
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults);
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
		assertThat(results.isEmpty()).isTrue();
	}

	@Test // DATAREDIS-417
	@Disabled
	@Override
	public void scanShouldReadEntireValueRangeWhenIndividualScanIterationsReturnEmptyCollection() {
		super.scanShouldReadEntireValueRangeWhenIndividualScanIterationsReturnEmptyCollection();
	}

	@Override
	@Test
	@Disabled
	public void xClaim() throws InterruptedException {
		super.xClaim();
	}

	@Test
	@Override
	@Disabled
	public void xPendingShouldLoadPendingMessagesForIdle() {}

	@Test
	@Override
	@Disabled
	public void xPendingShouldLoadPendingMessagesForIdleWithConsumer() {}

	@Override
	protected void initConnection() {
		connection.openPipeline();
	}

	@Override
	protected void verifyResults(List<Object> expected) {
		List<Object> expectedPipeline = new ArrayList<>();
		for (int i = 0; i < actual.size(); i++) {
			expectedPipeline.add(null);
		}
		assertThat(actual).isEqualTo(expectedPipeline);
		List<Object> results = getResults();
		assertThat(results).isEqualTo(expected);
	}

	@Override
	protected List<Object> getResults() {

		try {
			// we give redis some time to keep up
			Thread.sleep(10);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}

		return connection.closePipeline();
	}
}
