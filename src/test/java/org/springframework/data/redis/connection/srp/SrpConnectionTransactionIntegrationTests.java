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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;



/**
 * Integration test of {@link SrpConnection} functionality within a transaction
 *
 * @author Jennifer Hickey
 *
 */
public class SrpConnectionTransactionIntegrationTests extends SrpConnectionIntegrationTests {

	private boolean convert = true;

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
	 * operation. *
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

	@Test(expected = RedisSystemException.class)
	public void exceptionExecuteNative() throws Exception {
		super.exceptionExecuteNative();
	}

	// SRP tx.exec() is now returning converted data types b/c it uses pipeline, but
	// DefaultStringRedisConnection isn't converting byte[]s from exec() yet.
	// We implement the conversion in convertResult(), but convertResult's a bit over-eager in certain scenarios, hence these overrides

	@Test
	public void testExecute() {
		convert = false;
		super.testExecute();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
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

	@Test(expected = RedisSystemException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		connection.set("testing", "12");
		actual.add(connection.dump("testing".getBytes()));
		List<Object> results = getResults();
		initConnection();
		connection.restore("testing".getBytes(), 0, ((String)results.get(0)).getBytes());
		getResults();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		convert = false;
		super.testRestoreTtl();
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		convert = false;
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRangeByScore() {
		convert = false;
		super.testZRevRangeByScore();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		convert = false;
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		convert = false;
		super.testZRevRangeByScoreWithScores();
	}

	@Test(expected = UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptKill() {
		// Impossible to call script kill in a tx because you can't issue the
		// exec command while Redis is running a script
		connection.scriptKill();
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		List<Object> actual = connection.exec();
		List<Object> serializedResults = new ArrayList<Object>();
		for (Object result : actual) {
			Object convertedResult = convertResult(result);
			serializedResults.add(convertedResult);
		}
		return serializedResults;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		if(! convert) {
			return result;
		}
		if (result instanceof List && !(((List) result).isEmpty())
				&& ((List) result).get(0) instanceof byte[]) {
			return (SerializationUtils.deserialize((List<byte[]>) result, stringSerializer));
		} else if (result instanceof byte[]) {
			return (stringSerializer.deserialize((byte[]) result));
		} else if (result instanceof Map
				&& ((Map) result).keySet().iterator().next() instanceof byte[]) {
			return (SerializationUtils.deserialize((Map) result, stringSerializer));
		} else if (result instanceof Set && !(((Set) result).isEmpty())) {
			Object firstResult = ((Set) result).iterator().next();
			if(firstResult instanceof byte[]) {
				return (SerializationUtils.deserialize((Set) result, stringSerializer));
			}
			if(firstResult instanceof Tuple) {
				Set<StringTuple> tuples = new LinkedHashSet<StringTuple>();
				for (Tuple value : ((Set<Tuple>) result)) {
					DefaultStringTuple tuple = new DefaultStringTuple(value,stringSerializer.deserialize(value.getValue()));
					tuples.add(tuple);
				}
				return tuples;
			}
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
