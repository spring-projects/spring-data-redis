/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.hamcrest.core.IsNot;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.query.SortQueryBuilder;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration test of {@link RedisTemplate}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Anqing Shao
 * @author Duobiao Ou
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisTemplateTests<K, V> {

	@Autowired protected RedisTemplate<K, V> redisTemplate;

	protected ObjectFactory<K> keyFactory;

	protected ObjectFactory<V> valueFactory;

	public RedisTemplateTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
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

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
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
		assertThat(redisTemplate.boundValueOps(key1).get(), isEqual(value1));
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
		assertThat(redisTemplate.boundValueOps(key1).get(), isEqual(value1));
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
		K keyPattern = key1 instanceof String ? (K) "*" : (K) "*".getBytes();
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
		assertEquals(value, "it");
	}

	@Test
	public void testExec() {
		final K key1 = keyFactory.instance();
		final V value1 = valueFactory.instance();
		final K listKey = keyFactory.instance();
		final V listValue = valueFactory.instance();
		final K setKey = keyFactory.instance();
		final V setValue = valueFactory.instance();
		final K zsetKey = keyFactory.instance();
		final V zsetValue = valueFactory.instance();
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key1, value1);
				operations.opsForValue().get(key1);
				operations.opsForList().leftPush(listKey, listValue);
				operations.opsForList().range(listKey, 0l, 1l);
				operations.opsForSet().add(setKey, setValue);
				operations.opsForSet().members(setKey);
				operations.opsForZSet().add(zsetKey, zsetValue, 1d);
				operations.opsForZSet().rangeWithScores(zsetKey, 0l, -1l);
				return operations.exec();
			}
		});
		List<V> list = Collections.singletonList(listValue);
		Set<V> set = new HashSet<V>(Collections.singletonList(setValue));
		Set<TypedTuple<V>> tupleSet = new LinkedHashSet<TypedTuple<V>>(
				Collections.singletonList(new DefaultTypedTuple<V>(zsetValue, 1d)));
		assertThat(results, isEqual(Arrays.asList(new Object[] { value1, 1l, list, 1l, set, true, tupleSet })));
	}

	@Test
	public void testDiscard() {
		final K key1 = keyFactory.instance();
		final V value1 = valueFactory.instance();
		final V value2 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key1, value2);
				operations.discard();
				return null;
			}
		});
		assertThat(redisTemplate.boundValueOps(key1).get(), isEqual(value1));
	}

	@Test
	public void testExecCustomSerializer() {
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo", "5");
				operations.opsForValue().get("foo");
				operations.opsForValue().append("foo1", "5");
				operations.opsForList().leftPush("foolist", "6");
				operations.opsForList().range("foolist", 0l, 1l);
				operations.opsForSet().add("fooset", "7");
				operations.opsForSet().members("fooset");
				operations.opsForZSet().add("foozset", "9", 1d);
				operations.opsForZSet().rangeWithScores("foozset", 0l, -1l);
				operations.opsForZSet().range("foozset", 0, -1);
				operations.opsForHash().put("foomap", "10", "11");
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
		Map<Long, Long> map = new LinkedHashMap<Long, Long>();
		map.put(10l, 11l);
		assertThat(results,
				isEqual(Arrays.asList(new Object[] { 5l, 1L, 1l, list, 1l, longSet, true, tupleSet, zSet, true, map })));
	}

	@Test
	public void testExecConversionDisabled() {

		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setConvertPipelineAndTxResults(false);
		factory2.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory2);

		StringRedisTemplate template = new StringRedisTemplate(factory2);
		template.afterPropertiesSet();
		List<Object> results = template.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo", "bar");
				operations.opsForValue().get("foo");
				return operations.exec();
			}
		});
		// first value is "OK" from set call, results should still be in byte[]
		assertEquals("bar", new String((byte[]) results.get(1)));
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
		assertThat(results,
				isEqual(Arrays.asList(new Object[] { value1, 1l, 2l, Arrays.asList(new Object[] { listValue, listValue2 }) })));
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

		assertEquals(Arrays.asList(new Object[] { 5l, 1l, 2l, Arrays.asList(new Long[] { 10l, 11l }) }), results);
	}

	/**
	 * @see DATAREDIS-500
	 */
	@Test
	public void testExecutePipelinedWidthDifferentHashKeySerializerAndHashValueSerializer() {

		assumeTrue(redisTemplate instanceof StringRedisTemplate);

		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setHashKeySerializer(new GenericToStringSerializer<Long>(Long.class));
		redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<Person>(Person.class));

		Person person = new Person("Homer", "Simpson", 38);

		redisTemplate.opsForHash().put((K) "foo", 1L, person);

		List<Object> results = redisTemplate.executePipelined(new RedisCallback() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.hGetAll(((StringRedisSerializer) redisTemplate.getKeySerializer()).serialize("foo"));
				return null;
			}
		});

		assertEquals(((Map) results.get(0)).get(1L), person);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testExecutePipelinedNonNullRedisCallback() {
		redisTemplate.executePipelined(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) throws DataAccessException {
				return "Hey There";
			}
		});
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		assertThat(pipelinedResults,
				isEqual(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l, value1, 0l }), value1 })));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		}, new GenericToStringSerializer<Long>(Long.class));
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertEquals(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l, 5l, 0l }), 2l }), pipelinedResults);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testExecutePipelinedNonNullSessionCallback() {
		redisTemplate.executePipelined(new SessionCallback<String>() {
			@SuppressWarnings("rawtypes")
			public String execute(RedisOperations operations) throws DataAccessException {
				return "Whatup";
			}
		});
	}

	@Test
	public void testDelete() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		assertTrue(redisTemplate.hasKey(key1));
		redisTemplate.delete(key1);
		assertFalse(redisTemplate.hasKey(key1));
	}

	@Test
	public void testDeleteMultiple() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.opsForValue().set(key2, value2);
		List<K> keys = new ArrayList<K>();
		keys.add(key1);
		keys.add(key2);
		redisTemplate.delete(keys);
		assertFalse(redisTemplate.hasKey(key1));
		assertFalse(redisTemplate.hasKey(key2));
	}

	@Test
	public void testSort() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(value1 instanceof Number);
		redisTemplate.opsForList().rightPush(key1, value1);
		List<V> results = redisTemplate.sort(SortQueryBuilder.sort(key1).build());
		assertEquals(Collections.singletonList(value1), results);
	}

	@Test
	public void testSortStore() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(value1 instanceof Number);
		redisTemplate.opsForList().rightPush(key1, value1);
		assertEquals(Long.valueOf(1), redisTemplate.sort(SortQueryBuilder.sort(key1).build(), key2));
		assertEquals(Collections.singletonList(value1), redisTemplate.boundListOps(key2).range(0, -1));
	}

	@Test
	public void testSortBulkMapper() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(value1 instanceof Number);
		redisTemplate.opsForList().rightPush(key1, value1);
		List<String> results = redisTemplate.sort(SortQueryBuilder.sort(key1).get("#").build(),
				new BulkMapper<String, V>() {
					public String mapBulk(List<V> tuple) {
						return "FOO";
					}
				});
		assertEquals(Collections.singletonList("FOO"), results);
	}

	@Test
	public void testExpireAndGetExpireMillis() {

		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 500, TimeUnit.MILLISECONDS);

		assertTrue(redisTemplate.getExpire(key1, TimeUnit.MILLISECONDS) > 0l);
		// Timeout is longer because expire will be 1 sec if pExpire not supported
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey(key1));
			}
		}, 1500l);
	}

	@Test
	public void testGetExpireNoTimeUnit() {
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 2, TimeUnit.SECONDS);
		Long expire = redisTemplate.getExpire(key1);
		// Default behavior is to return seconds
		assertTrue(expire > 0l && expire <= 2l);
	}

	@Test
	public void testGetExpireSeconds() {
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 1500, TimeUnit.MILLISECONDS);
		assertEquals(Long.valueOf(1), redisTemplate.getExpire(key1, TimeUnit.SECONDS));
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	public void testGetExpireSecondsForKeyDoesNotExist() {

		Long expire = redisTemplate.getExpire(keyFactory.instance(), TimeUnit.SECONDS);
		assertTrue(expire < 0L);
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	public void testGetExpireSecondsForKeyExistButHasNoAssociatedExpire() {

		K key = keyFactory.instance();
		Long expire = redisTemplate.getExpire(key, TimeUnit.SECONDS);

		assertTrue(expire < 0L);
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	public void testGetExpireMillisForKeyDoesNotExist() {

		Long expire = redisTemplate.getExpire(keyFactory.instance(), TimeUnit.MILLISECONDS);

		assertTrue(expire < 0L);
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	public void testGetExpireMillisForKeyExistButHasNoAssociatedExpire() {

		K key = keyFactory.instance();
		redisTemplate.boundValueOps(key).set(valueFactory.instance());

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertTrue(expire < 0L);
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	public void testGetExpireMillis() {

		assumeTrue(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory
				|| redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		final K key = keyFactory.instance();
		redisTemplate.boundValueOps(key).set(valueFactory.instance());
		redisTemplate.expire(key, 1, TimeUnit.DAYS);

		Long ttl = redisTemplate.getExpire(key, TimeUnit.HOURS);

		assertThat(ttl, greaterThanOrEqualTo(23L));
		assertThat(ttl, lessThan(25L));
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testGetExpireMillisUsingTransactions() {

		assumeTrue(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory
				|| redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		final K key = keyFactory.instance();
		List<Object> result = redisTemplate.execute(new SessionCallback<List<Object>>() {

			@Override
			public List<Object> execute(RedisOperations operations) throws DataAccessException {

				operations.multi();
				operations.boundValueOps(key).set(valueFactory.instance());
				operations.expire(key, 1, TimeUnit.DAYS);
				operations.getExpire(key, TimeUnit.HOURS);

				return operations.exec();
			}
		});

		assertThat(result, hasSize(2));
		assertThat(((Long) result.get(1)), greaterThanOrEqualTo(23L));
		assertThat(((Long) result.get(1)), lessThan(25L));
	}

	/**
	 * @see DATAREDIS-526
	 */
	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testGetExpireMillisUsingPipelining() {

		assumeTrue(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory
				|| redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		final K key = keyFactory.instance();
		List<Object> result = redisTemplate.executePipelined(new SessionCallback<Object>() {

			@Override
			public Object execute(RedisOperations operations) throws DataAccessException {

				operations.boundValueOps(key).set(valueFactory.instance());
				operations.expire(key, 1, TimeUnit.DAYS);
				operations.getExpire(key, TimeUnit.HOURS);

				return null;
			}
		});

		assertThat(result, hasSize(2));
		assertThat(((Long) result.get(1)), greaterThanOrEqualTo(23L));
		assertThat(((Long) result.get(1)), lessThan(25L));
	}

	@Test
	public void testGetExpireMillisNotSupported() {

		assumeTrue(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory);

		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		assumeTrue(key1 instanceof String && value1 instanceof String);

		final StringRedisTemplate template2 = new StringRedisTemplate(redisTemplate.getConnectionFactory());
		template2.boundValueOps((String) key1).set((String) value1);
		template2.expire((String) key1, 5, TimeUnit.SECONDS);
		long expire = template2.getExpire((String) key1, TimeUnit.MILLISECONDS);
		// we should still get expire in milliseconds if requested
		assertTrue(expire > 1000 && expire <= 5000);
	}

	@Test
	public void testExpireAt() {
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expireAt(key1, new Date(System.currentTimeMillis() + 5l));
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey(key1));
			}
		}, 5l);
	}

	@Test
	public void testExpireAtMillisNotSupported() {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		assumeTrue(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory);

		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		assumeTrue(key1 instanceof String && value1 instanceof String);

		final StringRedisTemplate template2 = new StringRedisTemplate(redisTemplate.getConnectionFactory());
		template2.boundValueOps((String) key1).set((String) value1);
		template2.expireAt((String) key1, new Date(System.currentTimeMillis() + 5l));
		// Just ensure this works as expected, pExpireAt just adds some precision over expireAt
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!template2.hasKey((String) key1));
			}
		}, 5l);
	}

	@Test
	public void testPersist() throws Exception {
		// Test is meaningless in Redis 2.4 because key won't expire after 10 ms
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.expire(key1, 10, TimeUnit.MILLISECONDS);
		redisTemplate.persist(key1);
		Thread.sleep(10);
		assertTrue(redisTemplate.hasKey(key1));
	}

	@Test
	public void testRandomKey() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		assertThat(redisTemplate.randomKey(), isEqual(key1));
	}

	@Test
	public void testRename() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.rename(key1, key2);
		assertThat(redisTemplate.opsForValue().get(key2), isEqual(value1));
	}

	@Test
	public void testRenameIfAbsent() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.renameIfAbsent(key1, key2);
		assertTrue(redisTemplate.hasKey(key2));
	}

	@Test
	public void testType() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		assertEquals(DataType.STRING, redisTemplate.type(key1));
	}

	/**
	 * @see DATAREDIS-506
	 */
	@Test
	public void testWatch() {
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		final V value2 = valueFactory.instance();
		final V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);

		final Thread th = new Thread(new Runnable() {
			public void run() {
				redisTemplate.opsForValue().set(key1, value2);
			}
		});

		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {

				operations.watch(key1);

				th.start();
				try {
					th.join();
				} catch (InterruptedException e) {}

				operations.multi();
				operations.opsForValue().set(key1, value3);
				return operations.exec();
			}
		});

		if (redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory) {
			assertThat(results, is(empty()));
		} else {
			assertNull(results);
		}

		assertThat(redisTemplate.opsForValue().get(key1), isEqual(value2));
	}

	@Test
	public void testUnwatch() {

		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		final V value2 = valueFactory.instance();
		final V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		final Thread th = new Thread(new Runnable() {
			public void run() {
				redisTemplate.opsForValue().set(key1, value2);
			}
		});

		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {

				operations.watch(key1);

				th.start();
				try {
					th.join();
				} catch (InterruptedException e) {}

				operations.unwatch();
				operations.multi();
				operations.opsForValue().set(key1, value3);
				return operations.exec();
			}
		});

		assertTrue(results.isEmpty());
		assertThat(redisTemplate.opsForValue().get(key1), isEqual(value3));
	}

	/**
	 * @see DATAREDIS-506
	 */
	@Test
	public void testWatchMultipleKeys() {

		final K key1 = keyFactory.instance();
		final K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		final V value2 = valueFactory.instance();
		final V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);

		final Thread th = new Thread(new Runnable() {
			public void run() {
				redisTemplate.opsForValue().set(key1, value2);
			}
		});

		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {

				List<K> keys = new ArrayList<K>();
				keys.add(key1);
				keys.add(key2);
				operations.watch(keys);

				th.start();
				try {
					th.join();
				} catch (InterruptedException e) {}

				operations.multi();
				operations.opsForValue().set(key1, value3);
				return operations.exec();
			}
		});

		if (redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory) {
			assertThat(results, is(empty()));
		} else {
			assertNull(results);
		}

		assertThat(redisTemplate.opsForValue().get(key1), isEqual(value2));
	}

	@Test
	public void testConvertAndSend() {
		V value1 = valueFactory.instance();
		// Make sure basic message sent without Exception on serialization
		redisTemplate.convertAndSend("Channel", value1);
	}

	@Test
	public void testExecuteScriptCustomSerializers() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K key1 = keyFactory.instance();
		final DefaultRedisScript<String> script = new DefaultRedisScript<String>();
		script.setScriptText("return 'Hey'");
		script.setResultType(String.class);
		assertEquals("Hey", redisTemplate.execute(script, redisTemplate.getValueSerializer(), new StringRedisSerializer(),
				Collections.singletonList(key1)));
	}

	@Test
	public void clientListShouldReturnCorrectly() {
		assertThat(redisTemplate.getClientList().size(), IsNot.not(0));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private byte[] serialize(Object value, RedisSerializer serializer) {
		if (serializer == null && value instanceof byte[]) {
			return (byte[]) value;
		}
		return serializer.serialize(value);
	}
}
