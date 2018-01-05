/*
 * Copyright 2013-2018 the original author or authors.
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
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
abstract public class AbstractConnectionTransactionIntegrationTests extends AbstractConnectionIntegrationTests {

	@Ignore
	public void testMultiDiscard() {}

	@Ignore
	public void testMultiExec() {}

	@Ignore
	public void testUnwatch() {}

	@Ignore
	public void testWatch() {}

	@Ignore
	@Test
	public void testExecWithoutMulti() {}

	@Ignore
	@Test
	public void testErrorInTx() {}

	/*
	 * Using blocking ops inside a tx does not make a lot of sense as it would require blocking the
	 * entire server in order to execute the block atomically, which in turn does not allow other
	 * clients to perform a push operation. *
	 */

	@Ignore
	public void testBLPop() {}

	@Ignore
	public void testBRPop() {}

	@Ignore
	public void testBRPopLPush() {}

	@Ignore
	public void testBLPopTimeout() {}

	@Ignore
	public void testBRPopTimeout() {}

	@Ignore
	public void testBRPopLPushTimeout() {}

	@Ignore("Pub/Sub not supported with transactions")
	public void testPubSubWithNamedChannels() throws Exception {}

	@Ignore("Pub/Sub not supported with transactions")
	public void testPubSubWithPatterns() throws Exception {}

	@Ignore
	public void testNullKey() throws Exception {}

	@Ignore
	public void testNullValue() throws Exception {}

	@Ignore
	public void testHashNullKey() throws Exception {}

	@Ignore
	public void testHashNullValue() throws Exception {}

	@Test(expected = UnsupportedOperationException.class)
	public void testWatchWhileInTx() {
		connection.watch("foo".getBytes());
	}

	@Test(expected = UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testScriptKill() {
		// Impossible to call script kill in a tx because you can't issue the
		// exec command while Redis is running a script
		connection.scriptKill();
	}

	@Test // DATAREDIS-417
	@Ignore
	@Override
	public void scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection() {
		super.scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection();
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}

	protected void verifyResults(List<Object> expected) {
		List<Object> expectedTx = new ArrayList<>();
		for (int i = 0; i < actual.size(); i++) {
			expectedTx.add(null);
		}
		assertEquals(expectedTx, actual);
		List<Object> results = getResults();
		assertEquals(expected, results);
	}
}
