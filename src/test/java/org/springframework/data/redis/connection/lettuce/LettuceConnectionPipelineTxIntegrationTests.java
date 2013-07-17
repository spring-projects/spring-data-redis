package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.connection.convert.SetConverter;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link LettuceConnection} transactions within a pipeline
 *
 * @author Jennifer Hickey
 *
 */
public class LettuceConnectionPipelineTxIntegrationTests extends
		LettuceConnectionPipelineIntegrationTests {

	private boolean convert=true;

	private boolean convertToStringTuple=true;

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

	// Lettuce closePipeline() is now returning converted data types for executed txs, but
	// DefaultStringRedisConnection isn't converting byte[]s from exec() yet.
	// We implement the conversion in convertResult(), but convertResult's a bit over-eager in certain scenarios, hence these overrides

	@Test(expected = RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		convert = false;
		super.testRestoreExistingKey();
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
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		convert = false;
		super.testRestoreTtl();
	}

	@Test
	public void testExecute() {
		convert = false;
		super.testExecute();
	}

	@Test
	public void testExecuteNoArgs() {
		convert = false;
		super.testExecuteNoArgs();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testZRevRangeByScoreOffsetCount() {
		actual.add(byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d, 0, 5));
		assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "Bob", "James" })),(Set<String>) getResults().get(2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testZRevRangeByScore() {
		actual.add(byteConnection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(byteConnection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d));
		assertEquals(new LinkedHashSet<String>(Arrays.asList(new String[] { "Bob", "James" })), (Set<String>) getResults().get(2));
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		convertToStringTuple = false;
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		convertToStringTuple = false;
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
		connection.openPipeline();
		connection.multi();
	}

	protected List<Object> getResults() {
		assertNull(connection.exec());
		List<Object> pipelined = connection.closePipeline();
		List<Object> convertedResults = new ArrayList<Object>();
		for(Object result: pipelined) {
			convertedResults.add(convertResult(result));
		}
		return convertedResults;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		if(!convert) {
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
		}
		if (result instanceof Set && !(((Set) result).isEmpty())
				&& ((Set) result).iterator().next() instanceof Tuple) {
			if(convertToStringTuple) {
				return new SetConverter<Tuple, StringTuple>(new TupleConverter())
					.convert((Set)result);
			}
		}
		return result;
	}

	private class TupleConverter implements Converter<Tuple, StringTuple> {
		public StringTuple convert(Tuple source) {
			return new DefaultStringTuple(source, stringSerializer.deserialize(source.getValue()));
		}
	}

}
