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

package org.springframework.data.redis.connection.jredis;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.jredis.protocol.BulkResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link JredisConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JRedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {


	@Before
	public void setUp() {
		byteConnection = connectionFactory.getConnection();
		connection = new DefaultStringRedisConnection(byteConnection);
	}

	@After
	public void tearDown() {
		try {
			connection.flushDb();
			connection.close();
		} catch (DataAccessException e) {
			// Jredis closes a connection on Exception (which some tests
			// intentionally throw)
			// Attempting to close the connection again will result in error
			System.out.println("Connection already closed");
		}
		connection = null;
	}

	@Ignore("nulls are encoded to empty strings")
	public void testNullKey() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testNullValue() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testHashNullKey() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testHashNullValue() throws Exception {
	}

	@Ignore("Pub/Sub not supported")
	public void testPubSubWithPatterns() {
	}

	@Ignore("Pub/Sub not supported")
	public void testPubSubWithNamedChannels() {
	}

	@Ignore("DATAREDIS-129 Key search does not work with regex")
	public void testKeys() throws Exception {
	}

	@Ignore("DATAREDIS-171 JRedis StringIndexOutOfBoundsException in info() method in Redis 2.6")
	public void testInfo() throws Exception {
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBitSet() throws Exception {
		super.testBitSet();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testMultiExec() throws Exception {
		super.testMultiExec();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testMultiDiscard() throws Exception {
		super.testMultiDiscard();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testWatch() throws Exception {
		super.testWatch();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnwatch() throws Exception {
		super.testUnwatch();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBLPop() {
		super.testBLPop();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPop() {
		super.testBRPop();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLInsert() {
		super.testLInsert();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPopLPush() {
		super.testBRPopLPush();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLPushX() {
		super.testLPushX();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRPushX() {
		super.testRPushX();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetRangeSetRange() {
		super.testGetRangeSetRange();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testStrLen() {
		super.testStrLen();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testGetConfig() {
		super.testGetConfig();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStore() {
		super.testZInterStore();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRangeWithScores() {
		super.testZRangeWithScores();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRangeByScoreOffsetCount() {
		super.testZRangeByScoreOffsetCount();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRangeByScoreWithScores() {
		super.testZRangeByScoreWithScores();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRangeByScoreWithScoresOffsetCount() {
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRevRangeWithScores() {
		super.testZRevRangeWithScores();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStore() {
		super.testZUnionStore();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testHSetNX() throws Exception {
		super.testHSetNX();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testHIncrBy() {
		super.testHIncrBy();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testHMGetSet() {
		super.testHMGetSet();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPersist() throws Exception {
		super.testPersist();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSetEx() throws Exception {
		super.testSetEx();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPopTimeout() throws Exception {
		super.testBRPopTimeout();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBLPopTimeout() throws Exception {
		super.testBLPopTimeout();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPopLPushTimeout() throws Exception {
		super.testBRPopLPushTimeout();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRevRangeByScore() {
		super.testZRevRangeByScore();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRevRangeByScoreOffsetCount() {
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRevRangeByScoreWithScores() {
		super.testZRevRangeByScoreWithScores();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}


	// Jredis returns null for rPush
	@Test
	public void testSort() {
		connection.rPush("sortlist", "foo");
		connection.rPush("sortlist", "bar");
		connection.rPush("sortlist", "baz");
		assertEquals(Arrays.asList(new String[] { "bar", "baz", "foo" }),
				connection.sort("sortlist", new DefaultSortParameters(null, Order.ASC, true)));
	}

	@Test
	public void testSortStore() {
		connection.rPush("sortlist", "foo");
		connection.rPush("sortlist", "bar");
		connection.rPush("sortlist", "baz");
		assertEquals(Long.valueOf(3), connection.sort("sortlist", new DefaultSortParameters(null,
				Order.ASC, true), "newlist"));
		assertEquals(Arrays.asList(new String[] { "bar", "baz", "foo" }),
				connection.lRange("newlist", 0, 9));
	}

	@Test
	public void testSortNullParams() {
		connection.rPush("sortlist", "5");
		connection.rPush("sortlist", "2");
		connection.rPush("sortlist", "3");
		actual.add(connection.sort("sortlist", null));
		verifyResults(
			Arrays.asList(new Object[] { Arrays.asList(new String[] { "2", "3", "5" }) }), actual);
	}

	@Test
	public void testLPop() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "world");
		assertEquals("hello", connection.lPop("PopList"));
	}

	@Test
	public void testLRem() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "big");
		connection.rPush("PopList", "world");
		connection.rPush("PopList", "hello");
		assertEquals(Long.valueOf(2), connection.lRem("PopList", 2, "hello"));
		assertEquals(Arrays.asList(new String[] { "big", "world" }),
				connection.lRange("PopList", 0, -1));
	}

	@Test
	public void testLSet() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "big");
		connection.rPush("PopList", "world");
		connection.lSet("PopList", 1, "cruel");
		assertEquals(Arrays.asList(new String[] { "hello", "cruel", "world" }),
				connection.lRange("PopList", 0, -1));
	}

	@Test
	public void testLTrim() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "big");
		connection.rPush("PopList", "world");
		connection.lTrim("PopList", 1, -1);
		assertEquals(Arrays.asList(new String[] { "big", "world" }),
				connection.lRange("PopList", 0, -1));
	}

	@Test
	public void testRPop() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "world");
		assertEquals("world", connection.rPop("PopList"));
	}

	@Test
	public void testRPopLPush() {
		connection.rPush("PopList", "hello");
		connection.rPush("PopList", "world");
		connection.rPush("pop2", "hey");
		assertEquals("world", connection.rPopLPush("PopList", "pop2"));
		assertEquals(Arrays.asList(new String[] { "hello" }), connection.lRange("PopList", 0, -1));
		assertEquals(Arrays.asList(new String[] { "world", "hey" }),
				connection.lRange("pop2", 0, -1));
	}

	@Test
	public void testLIndex() {
		connection.lPush("testylist", "foo");
		assertEquals("foo", connection.lIndex("testylist", 0));
	}

	@Test
	public void testLPush() throws Exception {
		connection.lPush("testlist", "bar");
		connection.lPush("testlist", "baz");
		assertEquals(Arrays.asList(new String[] { "baz", "bar" }),
				connection.lRange("testlist", 0, -1));
	}

	@Test
	public void testExecute() {
		connection.set("foo", "bar");
		BulkResponse response = (BulkResponse) connection.execute("GET", JredisUtils.decode("foo".getBytes()));
		assertEquals("bar", stringSerializer.deserialize(response.getBulkData()));
	}

	@Test
	public void testMove() {
		connection.set("foo", "bar");
		actual.add(connection.move("foo", 1));
		verifyResults(
				Arrays.asList(new Object[] { true}), actual);
		// JRedis does not support select() on existing conn, create new one
		JredisConnectionFactory factory2 = new JredisConnectionFactory();
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection conn2 = new DefaultStringRedisConnection(factory2.getConnection());
		try {
			assertEquals("bar",conn2.get("foo"));
		} finally {
			if(conn2.exists("foo")) {
				conn2.del("foo");
			}
			conn2.close();
		}
	}

}