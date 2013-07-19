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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.reply.IntegerReply;
import redis.reply.Reply;
import redis.reply.StatusReply;



/**
 * Integration test of {@link SrpConnection} functionality within a transaction
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("SrpConnectionIntegrationTests-context.xml")
public class SrpConnectionTransactionIntegrationTests extends AbstractConnectionTransactionIntegrationTests {

	protected boolean convertResultToSet=false;

	protected boolean convertResultToStringTuples=false;

	protected boolean convertResultToTuples=false;

	protected boolean convertBooleanToLong=false;

	protected boolean convertResultsToMap=false;

	@Test
	public void testDel() {
		connection.set("testing","123");
		actual.add(connection.del("testing"));
		actual.add(connection.exists("testing"));
		verifyResults(Arrays.asList(new Object[] { true, false }));
	}

	@Test
	public void testSAdd() {
		convertResultToSet  = true;
		super.testSAdd();
	}

	@Test
	public void testSDiff() {
		convertResultToSet  = true;
		super.testSDiff();
	}

	@Test
	public void testSInter() {
		convertResultToSet  = true;
		super.testSInter();
	}

	@Test
	public void testSRem() {
		convertResultToSet  = true;
		super.testSRem();
	}

	@Test
	public void testSUnion() {
		convertResultToSet  = true;
		super.testSUnion();
	}

	@Test
	public void testSUnionStore() {
		convertResultToSet  = true;
		super.testSUnionStore();
	}

	@Test
	public void testSDiffStore() {
		convertResultToSet = true;
		super.testSDiffStore();
	}

	@Test
	public void testSInterStore() {
		convertResultToSet = true;
		super.testSInterStore();
	}

	@Test
	public void testZRemRangeByScore() {
		convertResultToSet = true;
		super.testZRemRangeByScore();
	}

	@Test
	public void testZAddAndZRange() {
		convertResultToSet = true;
		super.testZAddAndZRange();
	}

	@Test
	public void testZIncrBy() {
		convertResultToSet  = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zIncrBy("myset", 2, "Joe"));
		actual.add(connection.zRangeByScore("myset", 6, 6));
		verifyResults(
				Arrays.asList(new Object[] { true, true, true, "6",
						new LinkedHashSet<String>(Collections.singletonList("Joe")) }));
	}

	@Test
	public void testZInterStore() {
		convertResultToSet  = true;
		super.testZInterStore();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testHIncrByDouble() {
		actual.add(connection.hSet("test", "key", "2.9"));
		actual.add(connection.hIncrBy("test", "key", 3.5));
		actual.add(connection.hGet("test", "key"));
		// SRP returns byte[] from hIncrBy
		verifyResults(Arrays.asList(new Object[] { true, "6.4", "6.4" }));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testIncrByDouble() {
		connection.set("tdb", "4.5");
		actual.add(connection.incrBy("tdb", 7.2));
		actual.add(connection.get("tdb"));
		// SRP returns byte[] fro incrBy
		verifyResults(Arrays.asList(new Object[] { "11.7", "11.7" }));
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
	public void testBitCount() {
		convertBooleanToLong = true;
		super.testBitCount();
	}

	@Test
	public void testSortStoreNullParams() {
		convertBooleanToLong = true;
		super.testSortNullParams();
	}

	@Test
	public void testZRangeWithScores() {
		convertResultToStringTuples = true;
		convertResultToSet = true;
		super.testZRangeWithScores();
	}

	@Test
	public void testZRangeByScore() {
		convertResultToSet = true;
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		convertResultToSet = true;
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		convertResultToStringTuples = true;
		convertResultToSet = true;
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		convertResultToStringTuples = true;
		convertResultToSet = true;
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRange() {
		convertResultToSet = true;
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScores() {
		convertResultToStringTuples = true;
		convertResultToSet = true;
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		convertResultToTuples = true;
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	public void testZRem() {
		convertResultToSet = true;
		super.testZRem();
	}

	@Test
	public void testZRemRangeByRank() {
		convertResultToSet = true;
		super.testZRemRangeByRank();
	}

	@Test
	public void testZScore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 3, "Joe"));
		actual.add(connection.zScore("myset", "Joe"));
		verifyResults(Arrays.asList(new Object[] { true, true, true, "3" }));
	}

	@Test
	public void testZUnionStore() {
		convertResultToSet = true;
		super.testZUnionStore();
	}

	@Test
	public void testHSetGet() throws Exception {
		convertResultsToMap = true;
		super.testHSetGet();
	}

	@Test
	public void testHKeys() {
		convertResultToSet = true;
		super.testHKeys();
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaArrayStrings() {
		getResults();
		String sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}");
		initConnection();
		actual.add(connection.evalSha(sha1, ReturnType.MULTI, 1, "key1", "arg1"));
		List<Object> results = getResults();
		List<String> scriptResults = (List<String>) results.get(0);
		assertEquals(Arrays.asList(new Object[] { "key1", "arg1" }),
				scriptResults);
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnArrayStrings() {
		actual.add(connection.eval("return {KEYS[1],ARGV[1]}", ReturnType.MULTI, 1, "foo", "bar"));
		List<String> result = (List<String>) getResults().get(0);
		assertEquals(Arrays.asList(new Object[] { "foo", "bar" }), result);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnFalse() {
		actual.add(connection.eval("return false", ReturnType.BOOLEAN, 0));
		verifyResults(Arrays.asList(new Object[] { null }));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnArrayNumbers() {
		convertLongToBoolean = false;
		super.testEvalReturnArrayNumbers();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnArrayTrues() {
		convertLongToBoolean = false;
		super.testEvalReturnArrayTrues();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		Object convertedResult = super.convertResult(result);
		if (convertedResult instanceof Set && !(((Set) convertedResult).isEmpty())) {
			Object firstResult = ((Set) convertedResult).iterator().next();
			if(firstResult instanceof Tuple) {
				Set<StringTuple> tuples = new LinkedHashSet<StringTuple>();
				for (Tuple value : ((Set<Tuple>) convertedResult)) {
					DefaultStringTuple tuple = new DefaultStringTuple(value,stringSerializer.deserialize(value.getValue()));
					tuples.add(tuple);
				}
				return tuples;
			}
		}else if (convertedResult instanceof Reply[]) {
			if (convertResultToStringTuples) {
				Set<Tuple> tuples = SrpConverters.toTupleSet((Reply[])convertedResult);
				Collection<StringTuple> stringTuples;
				if(convertResultToSet) {
					stringTuples = new LinkedHashSet<StringTuple>();
				}else {
					stringTuples = new ArrayList<StringTuple>();
				}
				for (Tuple tuple : tuples) {
					stringTuples.add(new DefaultStringTuple(tuple, new String(tuple.getValue())));
				}
				return stringTuples;
			} else if(convertResultToTuples) {
				return SrpConverters.toTupleSet((Reply[])convertedResult);
			}
			else if (convertResultToSet) {
				return SerializationUtils.deserialize(SrpConverters.toBytesSet((Reply[])convertedResult),
						stringSerializer);
			} else if(((Reply[]) convertedResult).length > 0 && ((Reply[])convertedResult)[0] instanceof IntegerReply) {
				if(convertLongToBoolean) {
					return SrpConverters.toBooleanList((Reply[])convertedResult);
				}
				List<Long> results = new ArrayList<Long>();
				for(Reply reply: (Reply[])convertedResult) {
					results.add(((IntegerReply)reply).data());
				}
				return results;
			} else if(((Reply[]) convertedResult).length > 0 && ((Reply[])convertedResult)[0] instanceof StatusReply) {
				List<String> statuses = new ArrayList<String>();
				for(Reply reply: (Reply[])convertedResult) {
					statuses.add(((StatusReply)reply).data());
				}
				return statuses;
			} else if(convertResultsToMap) {
				return (SerializationUtils.deserialize(SrpConverters.toBytesMap((Reply[])convertedResult), stringSerializer));
			} else {
				return SerializationUtils.deserialize(
						SrpConverters.toBytesList((Reply[]) convertedResult), stringSerializer);
			}
		} else if(convertBooleanToLong && result instanceof Boolean) {
			if((Boolean)result) {
				return 1l;
			}
			return 0l;
		}
		return convertedResult;
	}

	@Override
	protected DataAccessException convertException(Exception ex) {
		return SrpConverters.toDataAccessException(ex);
	}
}
