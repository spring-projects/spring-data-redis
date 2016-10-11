/*
 * Copyright 2011-2015 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

import redis.reply.Reply;

/**
 * Integration test of {@link SrpConnection}
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author David Liu
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
public class SrpConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@After
	public void tearDown() {
		try {
			connection.flushAll();
		} catch (Exception e) {
			// SRP doesn't allow other commands to be executed once subscribed,
			// so
			// this fails after pub/sub tests
		}
		connection.close();
		connection = null;
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testExecuteNoArgs() {
		// SRP returns this as String while other drivers return as byte[]
		actual.add(connection.execute("PING"));
		verifyResults(Arrays.asList(new Object[] { "PONG" }));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayOKs() {
		// SRP returns the Strings from individual StatusReplys in a MultiBulkReply, while other clients return as byte[]
		actual.add(connection.eval("return { redis.call('set','abc','ghk'),  redis.call('set','abc','lfdf')}",
				ReturnType.MULTI, 0));
		verifyResults(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "OK", "OK" }) }));
	}

	@Test // DATAREDIS-285
	public void testExecuteShouldConvertArrayReplyCorrectly() {
		connection.set("spring", "awesome");
		connection.set("data", "cool");
		connection.set("redis", "supercalifragilisticexpialidocious");

		Object result = connection.execute("MGET", "spring".getBytes(), "data".getBytes(), "redis".getBytes());
		assertThat(result).isInstanceOf(Reply[].class);

		Reply<?>[] replies = (Reply[]) result;

		assertThat(replies[0].data()).isEqualTo("awesome".getBytes());
		assertThat(replies[1].data()).isEqualTo("cool".getBytes());
		assertThat(replies[2].data()).isEqualTo("supercalifragilisticexpialidocious".getBytes());
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayBytes() {
		getResults();
		byte[] sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}").getBytes();
		initConnection();
		actual.add(connection.evalSha(sha1, ReturnType.MULTI, 1, "key1".getBytes(), "arg1".getBytes()));
		List<Object> results = getResults();
		List<byte[]> scriptResults = (List<byte[]>) results.get(0);
		assertThat(scriptResults).contains("key1".getBytes(), "arg1".getBytes());
	}

	@Test // DATAREDIS-106
	public void zRangeByScoreTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<byte[]> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		assertThat(new String(zRangeByScore.iterator().next())).isEqualTo("two");
	}
}
