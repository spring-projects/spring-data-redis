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
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.Arrays;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * Integration test of {@link StringRedisTemplate}
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@ProfileValueSourceConfiguration(RedisTestProfileValueSource.class)
public class StringRedisTemplateTests {

	@Autowired
	private StringRedisTemplate redisTemplate;

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testDumpAndRestoreNoTtl() {
		redisTemplate.boundValueOps("testing").set("123");
		byte[] serializedValue = redisTemplate.dump("testing");
		assertNotNull(serializedValue);
		redisTemplate.delete("testing");
		redisTemplate.restore("testing", serializedValue, 0, TimeUnit.SECONDS);
		assertEquals("123", redisTemplate.boundValueOps("testing").get());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		redisTemplate.boundValueOps("testing").set("123");
		byte[] serializedValue = redisTemplate.dump("testing");
		assertNotNull(serializedValue);
		redisTemplate.delete("testing");
		redisTemplate.restore("testing", serializedValue, 200, TimeUnit.MILLISECONDS);
		assertEquals("123", redisTemplate.boundValueOps("testing").get());
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey("testing"));
			}
		}, 400);
	}

	@Test
	public void testKeys() throws Exception {
		redisTemplate.opsForValue().set("foo", "bar");
		assertNotNull(redisTemplate.keys("*"));
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
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo", "bar");
				// byte[]
				operations.opsForValue().get("foo");
				operations.opsForList().leftPush("foolist", "something");
				// List<byte[]>
				operations.opsForList().range("foolist", 0l, 1l);
				operations.opsForSet().add("fooset", "a");
				// Set<byte[]>
				operations.opsForSet().members("fooset");
				operations.opsForZSet().add("foozset", "Joe", 1d);
				// Set<TypedTuple>
				operations.opsForZSet().rangeWithScores("foozset", 0l, -1l);
				operations.opsForHash().put("foomap", "test", "passed");
				// Map<byte[],byte[]>
				operations.opsForHash().entries("foomap");
				return operations.exec();
			}
		});
		List<String> list = Collections.singletonList("something");
		Set<String> stringSet = new HashSet<String>(Collections.singletonList("a"));
		Set<TypedTuple<String>> tupleSet = new LinkedHashSet<TypedTuple<String>>(
				Collections.singletonList(new DefaultTypedTuple<String>("Joe", 1d)));
		Map<String,String> map = new LinkedHashMap<String,String>();
		map.put("test", "passed");
		assertEquals(Arrays.asList(new Object[] {"bar", 1l, list, true, stringSet, true, tupleSet, true, map}),
				results);
	}

	@Test
	public void testExecCustomSerializer() {
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
		List<Object> results = redisTemplate.executePipelined(new RedisCallback() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
				stringRedisConn.set("foo", "bar");
				stringRedisConn.get("foo");
				stringRedisConn.rPush("foolist", "a");
				stringRedisConn.rPush("foolist", "b");
				stringRedisConn.lRange("foolist", 0, -1);
				return null;
			}
		});
		assertEquals(Arrays.asList(new Object[] {"bar", 1l, 2l, Arrays.asList(new String[] {"a", "b"})}), results);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testExecutePipelinedCustomSerializer() {
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
		List<Object> pipelinedResults = redisTemplate.executePipelined(new SessionCallback() {
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForList().leftPush("foo", "bar");
				operations.opsForList().rightPop("foo");
				operations.opsForList().size("foo");
				operations.exec();
				operations.opsForValue().set("foo", "bar");
				operations.opsForValue().get("foo");
				return null;
			}
		});
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertEquals(Arrays.asList(new Object[] {Arrays.asList(new Object[] {1l, "bar", 0l}), "bar"}), pipelinedResults);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testExecutePipelinedTxCustomSerializer() {
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
}
