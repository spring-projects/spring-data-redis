/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import static org.assertj.core.api.Assertions.*;

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
 */
public class SrpConnectionPipelineTxIntegrationTests extends SrpConnectionTransactionIntegrationTests {

	@Test
	public void testUsePipelineAfterTxExec() {
		connection.set("foo", "bar");
		assertThat(connection.exec()).isNull();
		assertThat(connection.get("foo")).isNull();
		List<Object> results = connection.closePipeline();
		assertThat(results).isEqualTo(Arrays.asList(new Object[] { Collections.emptyList(), "bar" }));
		assertThat(connection.get("foo")).isEqualTo("bar");
	}

	@Test
	public void testExec2TransactionsInPipeline() {
		connection.set("foo", "bar");
		assertThat(connection.get("foo")).isNull();
		assertThat(connection.exec()).isNull();
		connection.multi();
		connection.set("foo", "baz");
		assertThat(connection.get("foo")).isNull();
		assertThat(connection.exec()).isNull();
		List<Object> results = connection.closePipeline();
		assertThat(results).hasSize(2);
		assertThat(results.get(0)).isEqualTo(Arrays.asList(new Object[] { "bar" }));
		assertThat(results.get(1)).isEqualTo(Arrays.asList(new Object[] { "baz" }));
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
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreBadData() {
		super.testRestoreBadData();
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

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	@SuppressWarnings("unchecked")
	protected List<Object> getResults() {
		assertThat(connection.exec()).isNull();
		List<Object> pipelined = connection.closePipeline();
		// We expect only the results of exec to be in the closed pipeline
		assertThat(pipelined).hasSize(1);
		List<Object> txResults = (List<Object>) pipelined.get(0);
		// Return exec results and this test should behave exactly like its superclass
		return txResults;
	}
}
