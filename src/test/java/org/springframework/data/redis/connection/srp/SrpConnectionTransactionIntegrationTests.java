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

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link SrpConnection} functionality within a transaction
 *
 * @author Jennifer Hickey
 *
 */
public class SrpConnectionTransactionIntegrationTests extends SrpConnectionPipelineIntegrationTests {

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
	@Test
	public void testWatch() {
	}

	/*
	 * Using blocking ops inside a tx does not make a lot of sense as it would
	 * require blocking the entire server in order to execute the block
	 * atomically, which in turn does not allow other clients to perform a push
	 * operation.
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

	@Test(expected = RedisSystemException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test
	@IfProfileValue(name = "redisVersion", values = { "2.4", "2.6" })
	public void testGetRangeSetRange() {
		connection.set("rangekey", "supercalifrag");
		actual.add(connection.getRange("rangekey", 0l, 2l));
		connection.setRange("rangekey", "ck", 2);
		actual.add(connection.get("rangekey"));
		verifyResults(Arrays.asList(new Object[] { "sup", 13l, "suckrcalifrag" }), actual);
	}

	@Test(expected=RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
		getResults();
	}

	@Test(expected=RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
		getResults();
	}

	@Test(expected=RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreBadData() {
		// Use something other than dump-specific serialization
		connection.restore("testing".getBytes(), 0, "foo".getBytes());
		getResults();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		connection.set("testing", "12");
		connection.dump("testing".getBytes());
		List<Object> results = getResults();
		initConnection();
		connection.restore("testing".getBytes(), 0, (byte[]) results.get(1));
		try {
			getResults();
			fail("Expected pipeline exception restoring an existing key");
		}catch(RedisSystemException e) {
		}
	}

	@Test(expected=RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitOpNotMultipleSources() {
		connection.set("key1", "abcd");
		connection.set("key2", "efgh");
		actual.add(connection.bitOp(BitOperation.NOT, "key3", "key1", "key2"));
		getResults();
	}

	@Test(expected = UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptKill() {
		// Impossible to call script kill in a tx because you can't issue the
		// exec command while Redis is running a script
		connection.scriptKill();
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
