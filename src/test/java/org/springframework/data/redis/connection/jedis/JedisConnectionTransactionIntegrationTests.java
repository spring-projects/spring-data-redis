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
package org.springframework.data.redis.connection.jedis;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.RedisVersionUtils;

/**
 * Integration test of {@link JedisConnection} transaction functionality.
 * <p>
 * Each method of {@link JedisConnection} behaves differently if executed with a
 * transaction (i.e. between multi and exec or discard calls), so this test
 * covers those branching points
 *
 * @author Jennifer Hickey
 *
 */
public class JedisConnectionTransactionIntegrationTests extends
		JedisConnectionPipelineIntegrationTests {

	@Ignore
	public void testMultiDiscard() {
	}

	@Ignore
	public void testMultiExec() {
	}

	@Ignore
	public void testUnwatch() {
	}

	@Ignore
	public void testWatch() {
	}

	/*
	 * Using blocking ops inside a tx does not make a lot of sense as it would
	 * require blocking the entire server in order to execute the block
	 * atomically, which in turn does not allow other clients to perform a push
	 * operation. *
	 */

	@Ignore
	public void testBLPop() {
	}

	@Ignore
	public void testBRPop() {
	}

	@Ignore
	public void testBRPopLPush() {
	}

	@Ignore
	public void testBLPopTimeout() {
	}

	@Ignore
	public void testBRPopTimeout() {
	}

	@Ignore
	public void testBRPopLPushTimeout() {
	}

	@Ignore
	public void testOpenPipelineTwice() {
	}

	@Ignore
	public void testClosePoolPipelinedDbSelect() {
	}

	// Unsupported Ops

	@Test(expected = RedisSystemException.class)
	public void testGetConfig() {
		connection.getConfig("*");
	}

	@Test(expected = RedisSystemException.class)
	public void testEcho() {
		super.testEcho();
	}

	@Test
	public void exceptionExecuteNative() throws Exception {
		actual.add(connection.execute("ZadD", getClass() + "#foo\t0.90\titem"));
		try {
			// Syntax error on queued commands are swallowed and no results are
			// returned
			verifyResults(Arrays.asList(new Object[] {}), actual);
			if(RedisVersionUtils.atLeast("2.6.5", connection)) {
				fail("Redis 2.6 should throw an Exception on exec if commands are invalid");
			}
		}catch(InvalidDataAccessApiUsageException e) {
		}
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
