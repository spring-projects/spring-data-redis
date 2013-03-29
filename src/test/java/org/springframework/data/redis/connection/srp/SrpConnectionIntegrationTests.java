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

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link SrpConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SrpConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@After
	public void tearDown() {
		try {
			connection.flushDb();
		} catch (Exception e) {
			// SRP doesn't allow other commands to be executed once subscribed,
			// so
			// this fails after pub/sub tests
		}
		connection.close();
		connection = null;
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testMultiExec() throws Exception {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testMultiDiscard() {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testWatch() {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testUnwatch() {
	}

	@Ignore("DATAREDIS-130, sort not working")
	public void testSort() {
	}

	@Ignore("DATAREDIS-130, sort not working")
	public void testSortStore() {
	}

	@Ignore("DATAREDIS-132 config get broken in SRP 0.2")
	public void testGetConfig() {
	}

	@Ignore("DATAREDIS-152 Syntax error on zRangeByScore and and zRangeByScoreWithScores when using offset and count")
	public void testZRangeByScoreOffsetCount() {
	}

	@Ignore("DATAREDIS-152 Syntax error on zRangeByScore and and zRangeByScoreWithScores when using offset and count")
	public void testZRangeByScoreWithScoresOffsetCount() {
	}

	@Ignore("DATAREDIS-156 SRP bRPopLPush ClassCastException")
	public void testBRPopLPushTimeout() {
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}
}
