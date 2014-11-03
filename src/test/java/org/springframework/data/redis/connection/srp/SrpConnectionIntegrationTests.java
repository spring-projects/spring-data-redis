/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.ZRangeOptions;
import org.springframework.data.redis.connection.jedis.JedisConverters;
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
			connection.flushDb();
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

	/**
	 * @see DATAREDIS-285
	 */
	@Test
	public void testExecuteShouldConvertArrayReplyCorrectly() {
		connection.set("spring", "awesome");
		connection.set("data", "cool");
		connection.set("redis", "supercalifragilisticexpialidocious");

		Object result = connection.execute("MGET", "spring".getBytes(), "data".getBytes(), "redis".getBytes());
		Assert.assertThat(result, IsInstanceOf.instanceOf(Reply[].class));

		Reply<?>[] replies = (Reply[]) result;

		Assert.assertThat(replies[0].data(), Is.<Object> is("awesome".getBytes()));
		Assert.assertThat(replies[1].data(), Is.<Object> is("cool".getBytes()));
		Assert.assertThat(replies[2].data(), Is.<Object> is("supercalifragilisticexpialidocious".getBytes()));
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
		assertEquals(Arrays.asList(new Object[] { "key1", "arg1" }),
				Arrays.asList(new Object[] { new String(scriptResults.get(0)), new String(scriptResults.get(1)) }));
	}

	/**
	 * @see DATAREDIS-106
	 */
	@Test
	public void zRangeByScoreTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<byte[]> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		Assert.assertEquals("two", new String(zRangeByScore.iterator().next()));
	}

	@Test
	public void zRangeOptionScoreInclusiveTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<byte[]> zRangeByScore = (Set<byte[]>) connection.zRange("myzset", new ZRangeOptions().score().greaterThanExclusive(1.1)
				.lessThanInclusive(3.0).build());
		Iterator<byte[]> iterator = zRangeByScore.iterator();
		assertEquals("two", JedisConverters.toString(iterator.next()));
		assertEquals("three", JedisConverters.toString(iterator.next()));
	}

	@Test
	public void zRangeOptionScoreExclusiveLimitWithScoresTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");
		connection.zAdd("myzset", 4, "four");

		Set<Tuple> zRangeByScore = (Set<Tuple>) connection.zRange("myzset", new ZRangeOptions().score().greaterThanInclusive(1.1)
				.lessThanInclusive(4.0).withScores().limitedTo().offset(1).count(1).build());
		assertEquals("three", JedisConverters.toString(zRangeByScore.iterator().next().getValue()));
		assertEquals(3.0, (double) zRangeByScore.iterator().next().getScore(), 0);
	}

	@Test
	public void zRangeOptionRevScoreExclusiveLimitTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");
		connection.zAdd("myzset", 4, "four");

		Set<byte[]> zRangeByScore = (Set<byte[]>) connection.zRange("myzset", new ZRangeOptions().score().rev().greaterThanInclusive(1.1)
				.lessThanInclusive(3.0).limitedTo().offset(0).count(2).build());

		Iterator<byte[]> iterator = zRangeByScore.iterator();
		assertEquals("three", JedisConverters.toString(iterator.next()));
		assertEquals("two", JedisConverters.toString(iterator.next()));
	}
}
