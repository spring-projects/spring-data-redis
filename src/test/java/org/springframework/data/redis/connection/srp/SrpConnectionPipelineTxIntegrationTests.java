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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.RedisVersionUtils;
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
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		try {
			getResults();
			fail("Expected an Exception to be thrown executing a command with syntax error");
		}catch(RedisPipelineException e) {
			// Redis 2.4, Exception occurs when we get the result of execute on closePipeline
			if(RedisVersionUtils.atLeast("2.6.4", byteConnection)) {
				fail("RedisPipelineException should not be thrown in Redis 2.6");
			}
		}catch(RedisSystemException e) {
			try {
				connection.closePipeline();
			}catch(Exception ex) {
				// Gonna get another error on closing the pipeline as results from execute
			}
			if(!RedisVersionUtils.atLeast("2.6", byteConnection)) {
				// Redis 2.6 returns an ErrorReploy on exec, the Exception occurs when we call exec()
				// b/c it waits on the response
				fail("RedisSystemException should only be thrown in Redis 2.6");
			}
		}
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

	@Test
	@IfProfileValue(name = "redisVersion", values = {"2.4", "2.6"})
	public void testGetRangeSetRange() {
		connection.set("rangekey", "supercalifrag");
		actual.add(connection.getRange("rangekey", 0l, 2l));
		connection.setRange("rangekey", "ck", 2);
		actual.add(connection.get("rangekey"));
		verifyResults(Arrays.asList(new Object[] { "sup", 13l, "suckrcalifrag" }), actual);
	}

	protected void initConnection() {
		connection.openPipeline();
		connection.multi();
	}

	protected List<Object> getResults() {
		assertNull(connection.exec());
		return connection.closePipeline();
	}
}
