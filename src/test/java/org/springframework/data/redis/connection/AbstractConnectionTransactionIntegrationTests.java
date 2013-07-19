package org.springframework.data.redis.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;

abstract public class AbstractConnectionTransactionIntegrationTests extends
		AbstractConnectionIntegrationTests {

	protected boolean convert = true;

	protected boolean convertLongToBoolean = true;

	protected boolean convertStringToProps = false;

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

	@Ignore
	@Test
	public void testExecWithoutMulti() {
	}

	@Ignore
	@Test
	public void testErrorInTx() {
	}

	/*
	 * Using blocking ops inside a tx does not make a lot of sense as it would require blocking the
	 * entire server in order to execute the block atomically, which in turn does not allow other
	 * clients to perform a push operation. *
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

	@Ignore("Pub/Sub not supported with transactions")
	public void testPubSubWithNamedChannels() throws Exception {
	}

	@Ignore("Pub/Sub not supported with transactions")
	public void testPubSubWithPatterns() throws Exception {
	}

	@Ignore
	public void testNullKey() throws Exception {
	}

	@Ignore
	public void testNullValue() throws Exception {
	}

	@Ignore
	public void testHashNullKey() throws Exception {
	}

	@Ignore
	public void testHashNullValue() throws Exception {
	}

	// Type conversion overrides

	@Test
	public void testExecute() {
		connection.set("foo", "bar");
		actual.add(connection.execute("GET", "foo"));
		assertEquals("bar", getResults().get(0));
	}

	@Test
	public void testExecuteNoArgs() {
		actual.add(connection.execute("PING"));
		assertEquals("PONG", getResults().get(0));
	}

	@Test
	public void testSort() {
		convertLongToBoolean = false;
		super.testSort();
	}

	@Test
	public void testSortStore() {
		convertLongToBoolean = false;
		super.testSortStore();
	}

	@Test
	public void testSortNullParams() {
		convertLongToBoolean = false;
		super.testSortNullParams();
	}

	@Test
	public void testDbSize() {
		convertLongToBoolean = false;
		super.testDbSize();
	}

	@Test
	public void testFlushDb() {
		convertLongToBoolean = false;
		super.testFlushDb();
	}

	@Test
	public void testDel() {
		convertLongToBoolean = false;
		super.testDel();
	}

	@Test
	public void testIncDecrByLong() {
		convertLongToBoolean = false;
		super.testIncrDecrByLong();
	}

	@Test
	public void testDecrByIncrBy() {
		convertLongToBoolean = false;
		super.testDecrByIncrBy();
	}

	@Test
	public void testIncrDecr() {
		convertLongToBoolean = false;
		super.testIncDecr();
	}

	@Test
	public void testLInsert() {
		convertLongToBoolean = false;
		super.testLInsert();
	}

	@Test
	public void testLRem() {
		convertLongToBoolean = false;
		super.testLRem();
	}

	@Test
	public void testLPop() {
		convertLongToBoolean = false;
		super.testLPop();
	}

	@Test
	public void testLLen() {
		convertLongToBoolean = false;
		super.testLLen();
	}

	@Test
	public void testLSet() {
		convertLongToBoolean = false;
		super.testLSet();
	}

	@Test
	public void testLTrim() {
		convertLongToBoolean = false;
		super.testLTrim();
	}

	@Test
	public void testRPop() {
		convertLongToBoolean = false;
		super.testRPop();
	}

	@Test
	public void testRPopLPush() {
		convertLongToBoolean = false;
		super.testRPopLPush();
	}

	@Test
	public void testLPushX() {
		convertLongToBoolean = false;
		super.testLPushX();
	}

	@Test
	public void testRPushX() {
		convertLongToBoolean = false;
		super.testRPushX();
	}

	@Test
	public void testLIndex() {
		convertLongToBoolean = false;
		super.testLIndex();
	}

	@Test
	public void testLPush() throws Exception {
		convertLongToBoolean = false;
		super.testLPush();
	}

	@Test
	public void testSDiffStore() {
		convertLongToBoolean = false;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sDiffStore("thirdset", "myset", "otherset"));
		actual.add(connection.sMembers("thirdset"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
				new HashSet<String>(Collections.singletonList("foo")) }));
	}

	@Test
	public void testSInterStore() {
		convertLongToBoolean = false;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sInterStore("thirdset", "myset", "otherset"));
		actual.add(connection.sMembers("thirdset"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
				new HashSet<String>(Collections.singletonList("bar")) }));
	}

	@Test
	public void testZRank() {
		convertLongToBoolean = false;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRank("myset", "James"));
		actual.add(connection.zRank("myset", "Bob"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 0l, 1l }));
	}

	@Test
	public void testZRemRangeByScore() {
		convertLongToBoolean = false;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRemRangeByScore("myset", 0d, 1d));
		actual.add(connection.zRange("myset", 0l, -1l));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l,
				new LinkedHashSet<String>(Arrays.asList(new String[] { "Bob" })) }));
	}

	@Test
	public void testZRevRank() {
		convertLongToBoolean = false;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 3, "Joe"));
		actual.add(connection.zRevRank("myset", "Joe"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 0l }));
	}

	@Test
	public void testIncrDecrByLong() {
		convertLongToBoolean = false;
		super.testIncrDecrByLong();
	}

	@Test
	public void testIncDecr() {
		convertLongToBoolean = false;
		super.testIncDecr();
	}

	@Test
	public void testType() {
		connection.set("something", "yo");
		actual.add(connection.type("something"));
		verifyResults(Arrays.asList(new Object[] { "string" }));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitCountNonExistentKey() {
		convertLongToBoolean = false;
		super.testBitCountNonExistentKey();
	}

	@Test
	public void testInfo() throws Exception {
		convertStringToProps = true;
		super.testInfo();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testInfoBySection() throws Exception {
		convertStringToProps = true;
		super.testInfoBySection();
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

	@Test
	public void testScriptLoadEvalSha() {
		convert = false;
		super.testScriptLoadEvalSha();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaArrayStrings() {
		convert = false;
		super.testEvalShaArrayStrings();
	}

	@Test(expected = RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		convert = false;
		super.testRestoreExistingKey();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnString() {
		convert = false;
		super.testEvalReturnString();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnArrayStrings() {
		convert = false;
		super.testEvalReturnArrayStrings();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleOK() {
		actual.add(connection.eval("return redis.call('set','abc','ghk')", ReturnType.STATUS, 0));
		assertEquals(Arrays.asList(new Object[] { "OK" }), getResultsNoConversion());
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
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		convert = false;
		super.testRestoreTtl();
	}

	@Test(expected = UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptKill() {
		// Impossible to call script kill in a tx because you can't issue the
		// exec command while Redis is running a script
		connection.scriptKill();
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
	public void testGetRangeSetRange() {
		connection.set("rangekey", "supercalifrag");
		actual.add(connection.getRange("rangekey", 0l, 2l));
		connection.setRange("rangekey", "ck", 2);
		actual.add(connection.get("rangekey"));
		// Lettuce returns a value for setRange
		verifyResults(Arrays.asList(new Object[] { "sup", 13l, "suckrcalifrag" }));
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

	protected void initConnection() {
		connection.multi();
	}

	protected abstract DataAccessException convertException(Exception ex);

	protected List<Object> getResults() {
		List<Object> actual = connection.exec();
		if (actual == null) {
			return null;
		}
		return convertResults(actual);
	}

	protected List<Object> getResultsNoConversion() {
		return connection.exec();
	}

	protected List<Object> convertResults(List<Object> results) {
		if(results == null) {
			return null;
		}
		List<Object> serializedResults = new ArrayList<Object>();
		for (Object result : results) {
			Object convertedResult = convertResult(result);
			if (convertedResult instanceof Exception) {
				throw convertException((Exception) convertedResult);
			}
			if (!"OK".equals(convertedResult) && !"QUEUED".equals(convertedResult)) {
				serializedResults.add(convertedResult);
			}
		}
		return serializedResults;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		if (!convert) {
			return result;
		}
		if (result instanceof List && !(((List) result).isEmpty())
				&& ((List) result).get(0) instanceof byte[]) {
			return (SerializationUtils.deserialize((List<byte[]>) result, stringSerializer));
		} else if (result instanceof byte[]) {
			if(convertStringToProps) {
				return Converters.toProperties(stringSerializer.deserialize((byte[]) result));
			}
			return (stringSerializer.deserialize((byte[]) result));
		} else if (result instanceof Map
				&& ((Map) result).keySet().iterator().next() instanceof byte[]) {
			return (SerializationUtils.deserialize((Map) result, stringSerializer));
		} else if (result instanceof Set && !(((Set) result).isEmpty())) {
			Object firstResult = ((Set) result).iterator().next();
			if (firstResult instanceof byte[]) {
				return (SerializationUtils.deserialize((Set) result, stringSerializer));
			}
		} else if (convertLongToBoolean && result instanceof Long) {
			if ((Long) result == 1l) {
				return true;
			} else if ((Long) result == 0l) {
				return false;
			} else {
				return result;
			}
		} else if (convertStringToProps && result instanceof String) {
			return Converters.toProperties((String) result);
		}
		return result;
	}

	protected void verifyResults(List<Object> expected) {
		List<Object> expectedTx = new ArrayList<Object>();
		for (int i = 0; i < actual.size(); i++) {
			expectedTx.add(null);
		}
		assertEquals(expectedTx, actual);
		List<Object> results = getResults();
		assertEquals(expected, results);
	}
}
