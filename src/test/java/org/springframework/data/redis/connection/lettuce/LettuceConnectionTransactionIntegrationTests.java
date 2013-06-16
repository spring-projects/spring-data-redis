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
package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lambdaworks.redis.RedisException;

/**
 * Integration test of {@link LettuceConnection} functionality within a
 * transaction
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("LettuceConnectionIntegrationTests-context.xml")
public class LettuceConnectionTransactionIntegrationTests extends
		LettuceConnectionPipelineIntegrationTests {

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
	 * operation. Also, Lettuce always times out in these scenarios b/c it waits
	 * for an actual response instead of accepting the null returned by op in tx
	 * *
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

	@Test
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		// Syntax error on queued commands are swallowed and no results are returned
		assertNull(getResults());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreBadData() {
		// Use something other than dump-specific serialization
		connection.restore("testing".getBytes(), 0, "foo".getBytes());
		List<Object> results = getResults();
		assertTrue(results.get(0) instanceof RedisException);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		connection.set("testing", "12");
		connection.dump("testing".getBytes());
		List<Object> results = getResults();
		initConnection();
		connection.restore("testing".getBytes(), 0, (byte[]) results.get(1));
		List<Object> restoreResults = getResults();
		assertTrue(restoreResults.get(0) instanceof RedisException);
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
