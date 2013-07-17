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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.connection.convert.SetConverter;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.ScoredValue;

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
		AbstractConnectionTransactionIntegrationTests {

	private boolean convertListToSet;

	private boolean convertToStringTuple=true;

	@Ignore
	public void testMultiConnectionsOneInTx() {
	}

	@Ignore
	public void testCloseBlockingOps() {
	}

	// Native Lettuce returns ZSets as Lists
	@Test
	public void testZAddAndZRange() {
		convertListToSet = true;
		super.testZAddAndZRange();
	}

	@Test
	public void testZIncrBy() {
		convertListToSet = true;
		super.testZIncrBy();
	}

	@Test
	public void testZInterStore() {
		convertListToSet = true;
		super.testZInterStore();
	}

	@Test
	public void testZInterStoreAggWeights() {
		convertListToSet = true;
		super.testZInterStoreAggWeights();
	}

	@Test
	public void testZRangeWithScores() {
		convertListToSet = true;
		super.testZRangeWithScores();
	}

	@Test
	public void testZRangeByScore() {
		convertListToSet = true;
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		convertListToSet = true;
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		convertListToSet = true;
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		convertListToSet = true;
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRange() {
		convertListToSet = true;
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScores() {
		convertListToSet = true;
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRem() {
		convertListToSet = true;
		super.testZRem();
	}

	@Test
	public void testZRemRangeByRank() {
		convertListToSet = true;
		super.testZRemRangeByRank();
	}

	@Test
	public void testZRemRangeByScore() {
		convertListToSet = true;
		super.testZRemRangeByScore();
	}

	@Test
	public void testZUnionStore() {
		convertListToSet = true;
		super.testZUnionStore();
	}

	@Test
	public void testZUnionStoreAggWeights() {
		convertListToSet = true;
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testBitSet() throws Exception {
		convertLongToBoolean = false;
		String key = "bitset-test";
		connection.setBit(key, 0, false);
		connection.setBit(key, 1, true);
		actual.add(connection.getBit(key, 0));
		actual.add(connection.getBit(key, 1));
		// Lettuce setBit returns Long instead of void
		verifyResults(Arrays.asList(new Object[] { 0l, 0l, 0l, 1l }));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitCount() {
		convertLongToBoolean = false;
		String key = "bitset-test";
		connection.setBit(key, 0, false);
		connection.setBit(key, 1, true);
		connection.setBit(key, 2, true);
		actual.add(connection.bitCount(key));
		// Lettuce setBit returns Long instead of void
		verifyResults(new ArrayList<Object>(Arrays.asList(0l, 0l, 0l, 2l)));
	}

	@Test
	public void testHKeys() {
		convertListToSet = true;
		super.testHKeys();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBitOpNotMultipleSources() {
		super.testBitOpNotMultipleSources();
	}

	@Test
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		// Syntax error on queued commands are swallowed and no results are
		// returned
		assertNull(getResults());
	}

	@Test
	public void testGetRangeSetRange() {
		connection.set("rangekey", "supercalifrag");
		actual.add(connection.getRange("rangekey", 0l, 2l));
		connection.setRange("rangekey", "ck", 2);
		actual.add(connection.get("rangekey"));
		// Lettuce returns a value for setRange
		verifyResults(Arrays.asList(new Object[] { "sup", 13l, "suckrcalifrag" }));
	}

	@Test(expected = UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountNegative() {
		super.testSRandMemberCountNegative();
	}

	@Test
	public void testSortStoreNullParams() {
		convertLongToBoolean = false;
		super.testSortStoreNullParams();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testZRevRangeByScoreOffsetCount() {
		actual.add(byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d, 0, 5));
		assertEquals(Arrays.asList(new String[] { "Bob", "James" }),(List<String>) getResults().get(2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testZRevRangeByScore() {
		actual.add(byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d));
		assertEquals(Arrays.asList(new String[] { "Bob", "James" }), (List<String>) getResults().get(2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		actual.add(byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d, 0, 5));
		assertEquals(Arrays.asList(new String[] { "Bob", "James" }),
				(List<byte[]>) getResults().get(2));
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		convertToStringTuple = false;
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleOK() {
		actual.add(connection.eval("return redis.call('set','abc','ghk')", ReturnType.STATUS, 0));
		assertEquals(Arrays.asList(new Object[] { "OK" }), connection.exec());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testDumpAndRestore() {
		convert = false;
		connection.set("testing", "12");
		actual.add(connection.dump("testing".getBytes()));
		List<Object> results = getResults();
		initConnection();
		actual.add(connection.del("testing"));
		actual.add((connection.get("testing")));
		connection.restore("testing".getBytes(), 0, (byte[]) results.get(results.size() - 1));
		actual.add(connection.get("testing"));
		results = getResults();
		assertEquals(3,results.size());
		assertEquals(1l, results.get(0));
		assertNull(results.get(1));
		assertEquals("12", new String((byte[])results.get(2)));
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnArrayOKs() {
		actual.add(connection.eval(
				"return { redis.call('set','abc','ghk'),  redis.call('set','abc','lfdf')}",
				ReturnType.MULTI, 0));
		List<String> result = (List<String>) getResults().get(0);
		assertEquals(Arrays.asList(new Object[] { "OK", "OK" }), result);
	}

	@Test
	public void testMove() {
		connection.set("foo", "bar");
		actual.add(connection.move("foo", 1));
		verifyResults(Arrays.asList(new Object[] { true}));
		// Lettuce does not support select when using shared conn, use a new conn factory
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory();
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Object convertResult(Object result) {
		if (!convert) {
			return result;
		}
		Object convertedResult = super.convertResult(result);
		if (convertedResult instanceof KeyValue) {
			List<String> keyValue = new ArrayList<String>();
			keyValue.add((String) super.convertResult(((KeyValue) convertedResult).key));
			keyValue.add((String) super.convertResult(((KeyValue) convertedResult).value));
			return keyValue;
		}
		if (convertedResult instanceof List && !(((List) result).isEmpty())
				&& ((List) convertedResult).get(0) instanceof ScoredValue) {
			Set<Tuple> tuples = LettuceConverters.toTupleSet((List) convertedResult);
			if(convertToStringTuple) {
				return new SetConverter<Tuple, StringTuple>(new TupleConverter())
					.convert(tuples);
			}
			return tuples;
		}
		if (convertListToSet && convertedResult instanceof List) {
			return new LinkedHashSet((List) convertedResult);
		}
		if (convertStringToProps && convertedResult instanceof String) {
			return LettuceConverters.toProperties((String) convertedResult);
		}
		return convertedResult;
	}

	private class TupleConverter implements Converter<Tuple, StringTuple> {
		public StringTuple convert(Tuple source) {
			return new DefaultStringTuple(source, stringSerializer.deserialize(source.getValue()));
		}
	}

	@Override
	protected DataAccessException convertException(Exception ex) {
		return LettuceConverters.toDataAccessException(ex);
	}
}
