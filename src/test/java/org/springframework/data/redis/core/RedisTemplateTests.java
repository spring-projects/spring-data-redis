/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 *
 * Integration test of {@link RedisTemplate}
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(Parameterized.class)
public class RedisTemplateTests<K,V> {

	@Autowired
	private RedisTemplate<K,V> redisTemplate;
	
	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	public RedisTemplateTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@Test
	public void testDumpAndRestoreNoTtl() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		byte[] serializedValue = redisTemplate.dump(key1);
		assertNotNull(serializedValue);
		redisTemplate.delete(key1);
		redisTemplate.restore(key1, serializedValue, 0, TimeUnit.SECONDS);
		assertEquals(value1, redisTemplate.boundValueOps(key1).get());
	}

	@Test
	public void testRestoreTtl() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		byte[] serializedValue = redisTemplate.dump(key1);
		assertNotNull(serializedValue);
		redisTemplate.delete(key1);
		redisTemplate.restore(key1, serializedValue, 200, TimeUnit.MILLISECONDS);
		assertEquals(value1, redisTemplate.boundValueOps(key1).get());
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey(key1));
			}
		}, 400);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testKeys() throws Exception {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(key1 instanceof String || key1 instanceof byte[]);
		redisTemplate.opsForValue().set(key1, value1);
		K keyPattern = key1 instanceof String ? (K) "*" : (K)"*".getBytes();
		assertNotNull(redisTemplate.keys(keyPattern));
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = IllegalArgumentException.class)
	public void testTemplateNotInitialized() throws Exception {
		RedisTemplate tpl = new RedisTemplate();
		tpl.setConnectionFactory(redisTemplate.getConnectionFactory());
		tpl.exec();
	}

	@Test
	public void testStringTemplateExecutesWithStringConn() {
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		String value = redisTemplate.execute(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) {
				StringRedisConnection stringConn = (StringRedisConnection) connection;
				stringConn.set("test", "it");
				return stringConn.get("test");
			}
		});
		assertEquals(value,"it");
	}

	@Test
	public void testExecDeserializes() {
		final K key1 = keyFactory.instance();
		final V value1 = valueFactory.instance();
		final K listKey = keyFactory.instance();
		final V listValue = valueFactory.instance();
		final K setKey = keyFactory.instance();
		final V setValue = valueFactory.instance();
		final K zsetKey = keyFactory.instance();
		final V zsetValue = valueFactory.instance();
		final K mapKey = keyFactory.instance();
		final Object hashKey = redisTemplate.getHashKeySerializer().deserialize("test".getBytes());
		final Object hashValue = redisTemplate.getHashValueSerializer().deserialize("passed".getBytes());
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key1, value1);
				// byte[]
				operations.opsForValue().get(key1);
				operations.opsForList().leftPush(listKey, listValue);
				// List<byte[]>
				operations.opsForList().range(listKey, 0l, 1l);
				operations.opsForSet().add(setKey, setValue);
				// Set<byte[]>
				operations.opsForSet().members(setKey);
				operations.opsForZSet().add(zsetKey, zsetValue, 1d);
				// Set<TypedTuple>
				operations.opsForZSet().rangeWithScores(zsetKey, 0l, -1l);
				operations.opsForHash().put(mapKey, hashKey, hashValue);
				// Map<byte[],byte[]>
				operations.opsForHash().entries(mapKey);
				return operations.exec();
			}
		});
		List<V> list = Collections.singletonList(listValue);
		Set<V> set = new HashSet<V>(Collections.singletonList(setValue));
		Set<TypedTuple<V>> tupleSet = new LinkedHashSet<TypedTuple<V>>(
				Collections.singletonList(new DefaultTypedTuple<V>(zsetValue, 1d)));
		Map<Object,Object> map = new LinkedHashMap<Object,Object>();
		map.put(hashKey, hashValue);
		assertEquals(Arrays.asList(new Object[] {value1, 1l, list, true, set, true, tupleSet, true, map}),
				results);
	}

	@Test
	public void testExecCustomSerializer() {
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo", "5");
				// byte[]
				operations.opsForValue().get("foo");
				operations.opsForList().leftPush("foolist", "6");
				// List<byte[]>
				operations.opsForList().range("foolist", 0l, 1l);
				operations.opsForSet().add("fooset", "7");
				// Set<byte[]>
				operations.opsForSet().members("fooset");
				operations.opsForZSet().add("foozset", "9", 1d);
				// Set<TypedTuple>
				operations.opsForZSet().rangeWithScores("foozset", 0l, -1l);
				operations.opsForZSet().range("foozset", 0, -1);
				operations.opsForHash().put("foomap", "10", "11");
				// Map<byte[],byte[]>
				operations.opsForHash().entries("foomap");
				return operations.exec(new GenericToStringSerializer<Long>(Long.class));
			}
		});
		// Everything should be converted to Longs
		List<Long> list = Collections.singletonList(6l);
		Set<Long> longSet = new HashSet<Long>(Collections.singletonList(7l));
		Set<TypedTuple<Long>> tupleSet = new LinkedHashSet<TypedTuple<Long>>(
				Collections.singletonList(new DefaultTypedTuple<Long>(9l, 1d)));
		Set<Long> zSet = new LinkedHashSet<Long>(Collections.singletonList(9l));
		Map<Long, Long> map = new LinkedHashMap<Long,Long>();
		map.put(10l, 11l);
		assertEquals(Arrays.asList(new Object[] {5l, 1l, list, true, longSet, true, tupleSet, zSet, true, map}),
				results);
	}

	@Test
	public void testExecConversionDisabled() {
		SrpConnectionFactory factory2 = new SrpConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setConvertPipelineAndTxResults(false);
		factory2.afterPropertiesSet();
		StringRedisTemplate template = new StringRedisTemplate(factory2);
		template.afterPropertiesSet();
		List<Object> results = template.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo","bar");
				operations.opsForValue().get("foo");
				return operations.exec();
			}
		});
		// first value is "OK" from set call, results should still be in byte[]
		assertEquals("bar", new String((byte[])results.get(1)));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testExecutePipelined() {
		final K key1 = keyFactory.instance();
		final V value1 = valueFactory.instance();
		final K listKey = keyFactory.instance();
		final V listValue = valueFactory.instance();
		final V listValue2 = valueFactory.instance();
		List<Object> results = redisTemplate.executePipelined(new RedisCallback() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				byte[] rawKey = serialize(key1, redisTemplate.getKeySerializer());
				byte[] rawListKey = serialize(listKey, redisTemplate.getKeySerializer());
				connection.set(rawKey, serialize(value1, redisTemplate.getValueSerializer()));
				connection.get(rawKey);
				connection.rPush(rawListKey, serialize(listValue, redisTemplate.getValueSerializer()));
				connection.rPush(rawListKey, serialize(listValue2, redisTemplate.getValueSerializer()));
				connection.lRange(rawListKey, 0, -1);
				return null;
			}
		});
		assertEquals(Arrays.asList(new Object[] {value1, 1l, 2l, Arrays.asList(new Object[] {listValue, listValue2})}), results);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testExecutePipelinedCustomSerializer() {
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		List<Object> results = redisTemplate.executePipelined(new RedisCallback() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
				stringRedisConn.set("foo", "5");
				stringRedisConn.get("foo");
				stringRedisConn.rPush("foolist", "10");
				stringRedisConn.rPush("foolist", "11");
				stringRedisConn.lRange("foolist", 0, -1);
				return null;
			}
		}, new GenericToStringSerializer<Long>(Long.class));
		assertEquals(Arrays.asList(new Object[] {5l, 1l, 2l, Arrays.asList(new Long[] {10l, 11l})}), results);
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testExecutePipelinedNonNullRedisCallback() {
		redisTemplate.executePipelined(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) throws DataAccessException {
				return "Hey There";
			}
		});
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testExecutePipelinedTx() {
		final K key1 = keyFactory.instance();
		final V value1 = valueFactory.instance();
		List<Object> pipelinedResults = redisTemplate.executePipelined(new SessionCallback() {
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForList().leftPush(key1, value1);
				operations.opsForList().rightPop(key1);
				operations.opsForList().size(key1);
				operations.exec();
				operations.opsForValue().set(key1, value1);
				operations.opsForValue().get(key1);
				return null;
			}
		});
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertEquals(Arrays.asList(new Object[] {Arrays.asList(new Object[] {1l, value1, 0l}), value1}), pipelinedResults);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testExecutePipelinedTxCustomSerializer() {
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		List<Object> pipelinedResults = redisTemplate.executePipelined(new SessionCallback() {
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForList().leftPush("fooList", "5");
				operations.opsForList().rightPop("fooList");
				operations.opsForList().size("fooList");
				operations.exec();
				operations.opsForValue().set("foo", "2");
				operations.opsForValue().get("foo");
				return null;
			}
		},new GenericToStringSerializer<Long>(Long.class));
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertEquals(Arrays.asList(new Object[] {Arrays.asList(new Object[] {1l, 5l, 0l}), 2l}), pipelinedResults);
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testExecutePipelinedNonNullSessionCallback() {
		redisTemplate.executePipelined(new SessionCallback<String>() {
			@SuppressWarnings("rawtypes")
			public String execute(RedisOperations operations) throws DataAccessException {
				return "Whatup";
			}
		});
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private byte[] serialize(Object value, RedisSerializer serializer) {
		return serializer.serialize(value);
	}
}
