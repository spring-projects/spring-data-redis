/*
 * Copyright 2013-2022 the original author or authors.
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

import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
abstract public class AbstractConnectionTransactionIntegrationTests extends AbstractConnectionIntegrationTests {

	@Test
	@Override
	@Disabled
	public void testMultiDiscard() {}

	@Test
	@Override
	@Disabled
	public void testMultiExec() {}

	@Test
	@Override
	@Disabled
	public void testUnwatch() {}

	@Test
	@Override
	@Disabled
	public void testWatch() {}

	@Override
	@Disabled
	@Test
	public void testExecWithoutMulti() {}

	@Override
	@Disabled
	@Test
	public void testErrorInTx() {}

	/*
	 * Using blocking ops inside a tx does not make a lot of sense as it would require blocking the
	 * entire server in order to execute the block atomically, which in turn does not allow other
	 * clients to perform a push operation. *
	 */

	@Test
	@Override
	@Disabled
	public void testBLPop() {}

	@Test
	@Override
	@Disabled
	public void testBRPop() {}

	@Test
	@Override
	@Disabled
	public void testBRPopLPush() {}

	@Test
	@Override
	@Disabled
	public void testBLPopTimeout() {}

	@Test
	@Override
	@Disabled
	public void testBRPopTimeout() {}

	@Test
	@Override
	@Disabled
	public void testBRPopLPushTimeout() {}

	@Test
	@Override
	@Disabled("Pub/Sub not supported with transactions")
	public void testPubSubWithNamedChannels() throws Exception {}

	@Test
	@Override
	@Disabled("Pub/Sub not supported with transactions")
	public void testPubSubWithPatterns() throws Exception {}

	@Test
	@Override
	@Disabled
	public void testNullKey() {}

	@Test
	@Override
	@Disabled
	public void testNullValue() {}

	@Test
	@Override
	@Disabled
	public void testHashNullKey() {}

	@Test
	@Override
	@Disabled
	public void testHashNullValue() {}

	@Test
	public void testWatchWhileInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.watch("foo".getBytes()));
	}

	@Test
	public void testScriptKill() {
		// Impossible to call script kill in a tx because you can't issue the
		// exec command while Redis is running a script
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> connection.scriptKill());
	}

	@Test // DATAREDIS-417
	@Disabled
	@Override
	public void scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection() {
		super.scanShouldReadEntireValueRangeWhenIdividualScanIterationsReturnEmptyCollection();
	}

	@Override
	@Test
	@Disabled
	public void xClaim() throws InterruptedException {
		super.xClaim();
	}

	@Override
	protected void initConnection() {
		connection.multi();
	}

	@Override
	protected List<Object> getResults() {
		return connection.exec();
	}

	@Override
	protected void verifyResults(List<Object> expected) {
		List<Object> expectedTx = new ArrayList<>();
		for (int i = 0; i < actual.size(); i++) {
			expectedTx.add(null);
		}
		assertThat(actual).isEqualTo(expectedTx);
		List<Object> results = getResults();
		assertThat(results).isEqualTo(expected);
	}
}
