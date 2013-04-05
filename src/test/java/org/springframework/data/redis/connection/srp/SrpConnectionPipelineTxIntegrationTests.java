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

import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisPipelineException;

import redis.client.RedisClientBase;

/**
 * Integration test of {@link SrpConnection} transactions within a pipeline
 *
 * @author Jennifer Hickey
 *
 */
public class SrpConnectionPipelineTxIntegrationTests extends
		SrpConnectionTransactionIntegrationTests {

	@Test(expected = RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test
	public void testUsePipelineAfterTxExec() {
		connection.set("foo", "bar");
		assertNull(connection.exec());
		assertNull(connection.get("foo"));
		List<Object> results = connection.closePipeline();
		assertEquals(2, results.size());
		assertEquals("OK", results.get(0));
		assertEquals("bar", new String((byte[]) results.get(1)));
		assertEquals("bar", connection.get("foo"));
	}

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
		assertEquals(4, results.size());
		assertEquals("OK", results.get(0));
		assertEquals("bar", new String((byte[]) results.get(1)));
		assertEquals("OK", results.get(2));
		assertEquals("baz", new String((byte[]) results.get(3)));
	}

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	protected List<Object> getResults() {
		assertNull(connection.exec());
		return connection.closePipeline();
	}

	protected int getRedisVersion() {
		connection.exec();
		connection.closePipeline();
		int version = RedisClientBase.parseVersion((String) connection.info()
				.get("redis_version"));
		initConnection();
		return version;
	}
}
