/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;
import static org.awaitility.Awaitility.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterConnection;
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
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.data.redis.test.util.CollectionAwareComparator;

/**
 * Integration test of {@link RedisTemplate}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Anqing Shao
 * @author Duobiao Ou
 * @author Mark Paluch
 * @author ihaohong
 * @author Hendrik Duerkop
 * @author Chen Li
 */
@MethodSource("testParams")
public class RedisTemplateIntegrationTests<K, V> {

	protected final RedisTemplate<K, V> redisTemplate;
	protected final ObjectFactory<K> keyFactory;
	protected final ObjectFactory<V> valueFactory;

	RedisTemplateIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@BeforeEach
	void setUp() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@ParameterizedRedisTest
	void testDumpAndRestoreNoTtl() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		byte[] serializedValue = redisTemplate.dump(key1);
		assertThat(serializedValue).isNotNull();
		redisTemplate.delete(key1);
		redisTemplate.restore(key1, serializedValue, 0, TimeUnit.SECONDS);
		assertThat(redisTemplate.boundValueOps(key1).get()).isEqualTo(value1);
	}

	@ParameterizedRedisTest
	void testRestoreTtl() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		byte[] serializedValue = redisTemplate.dump(key1);
		assertThat(serializedValue).isNotNull();
		redisTemplate.delete(key1);
		redisTemplate.restore(key1, serializedValue, 10000, TimeUnit.MILLISECONDS);
		assertThat(redisTemplate.boundValueOps(key1).get()).isEqualTo(value1);
		assertThat(redisTemplate.getExpire(key1)).isGreaterThan(1L);
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testKeys() throws Exception {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeThat(key1 instanceof String || key1 instanceof byte[]).isTrue();
		redisTemplate.opsForValue().set(key1, value1);
		K keyPattern = key1 instanceof String ? (K) "*" : (K) "*".getBytes();
		assertThat(redisTemplate.keys(keyPattern)).isNotNull();
	}

	@ParameterizedRedisTest // GH-2260
	void testScan() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeThat(key1 instanceof String || key1 instanceof byte[]).isTrue();
		redisTemplate.opsForValue().set(key1, value1);
		long count = 0;
		try (Cursor<K> cursor = redisTemplate.scan(ScanOptions.scanOptions().count(1).build())) {
			while (cursor.hasNext()) {
				assertThat(cursor.next()).isEqualTo(key1);
				count++;
			}
		}
		assertThat(count).isEqualTo(1);
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedRedisTest
	void testTemplateNotInitialized() throws Exception {
		RedisTemplate tpl = new RedisTemplate();
		tpl.setConnectionFactory(redisTemplate.getConnectionFactory());
		assertThatIllegalArgumentException().isThrownBy(() -> tpl.exec());
	}

	@ParameterizedRedisTest
	void testStringTemplateExecutesWithStringConn() {
		assumeThat(redisTemplate instanceof StringRedisTemplate).isTrue();
		String value = redisTemplate.execute((RedisCallback<String>) connection -> {
			StringRedisConnection stringConn = (StringRedisConnection) connection;
			stringConn.set("test", "it");
			return stringConn.get("test");
		});
		assertThat("it").isEqualTo(value);
	}

	@ParameterizedRedisTest
	public void testExec() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		K listKey = keyFactory.instance();
		V listValue = valueFactory.instance();
		K setKey = keyFactory.instance();
		V setValue = valueFactory.instance();
		K zsetKey = keyFactory.instance();
		V zsetValue = valueFactory.instance();
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set(key1, value1);
				operations.opsForValue().get(key1);
				operations.opsForList().leftPush(listKey, listValue);
				operations.opsForList().range(listKey, 0L, 1L);
				operations.opsForSet().add(setKey, setValue);
				operations.opsForSet().members(setKey);
				operations.opsForZSet().add(zsetKey, zsetValue, 1d);
				operations.opsForZSet().rangeWithScores(zsetKey, 0L, -1L);
				return operations.exec();
			}
		});
		List<V> list = Collections.singletonList(listValue);
		Set<V> set = new HashSet<>(Collections.singletonList(setValue));
		Set<TypedTuple<V>> tupleSet = new LinkedHashSet<>(
				Collections.singletonList(new DefaultTypedTuple<>(zsetValue, 1d)));

		assertThat(results).usingElementComparator(CollectionAwareComparator.INSTANCE).containsExactly(true, value1, 1L,
				list, 1L, set, true, tupleSet);
	}

	@ParameterizedRedisTest
	public void testDiscard() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
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
		assertThat(redisTemplate.boundValueOps(key1).get()).isEqualTo(value1);
	}

	@ParameterizedRedisTest
	void testExecCustomSerializer() {
		assumeThat(redisTemplate instanceof StringRedisTemplate).isTrue();
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForValue().set("foo", "5");
				operations.opsForValue().get("foo");
				operations.opsForValue().append("foo1", "5");
				operations.opsForList().leftPush("foolist", "6");
				operations.opsForList().range("foolist", 0L, 1L);
				operations.opsForSet().add("fooset", "7");
				operations.opsForSet().members("fooset");
				operations.opsForZSet().add("foozset", "9", 1d);
				operations.opsForZSet().rangeWithScores("foozset", 0L, -1L);
				operations.opsForZSet().range("foozset", 0, -1);
				operations.opsForHash().put("foomap", "10", "11");
				operations.opsForHash().entries("foomap");
				return operations.exec(new GenericToStringSerializer<>(Long.class));
			}
		});
		// Everything should be converted to Longs
		List<Long> list = Collections.singletonList(6L);
		Set<Long> longSet = new HashSet<>(Collections.singletonList(7L));
		Set<TypedTuple<Long>> tupleSet = new LinkedHashSet<>(Collections.singletonList(new DefaultTypedTuple<>(9L, 1d)));
		Set<Long> zSet = new LinkedHashSet<>(Collections.singletonList(9L));
		Map<Long, Long> map = new LinkedHashMap<>();
		map.put(10L, 11L);

		assertThat(results).containsExactly(true, 5L, 1L, 1L, list, 1L, longSet, true, tupleSet, zSet, true, map);
	}

	@ParameterizedRedisTest
	public void testExecConversionDisabled() {

		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
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
		assertThat(new String((byte[]) results.get(1))).isEqualTo("bar");
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedRedisTest
	public void testExecutePipelined() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		K listKey = keyFactory.instance();
		V listValue = valueFactory.instance();
		V listValue2 = valueFactory.instance();
		List<Object> results = redisTemplate.executePipelined((RedisCallback) connection -> {
			byte[] rawKey = serialize(key1, redisTemplate.getKeySerializer());
			byte[] rawListKey = serialize(listKey, redisTemplate.getKeySerializer());
			connection.set(rawKey, serialize(value1, redisTemplate.getValueSerializer()));
			connection.get(rawKey);
			connection.rPush(rawListKey, serialize(listValue, redisTemplate.getValueSerializer()));
			connection.rPush(rawListKey, serialize(listValue2, redisTemplate.getValueSerializer()));
			connection.lRange(rawListKey, 0, -1);
			return null;
		});
		assertThat(results).usingElementComparator(CollectionAwareComparator.INSTANCE).containsExactly(true, value1, 1L, 2L,
				Arrays.asList(listValue, listValue2));
	}

	@SuppressWarnings("rawtypes")
	@ParameterizedRedisTest
	void testExecutePipelinedCustomSerializer() {

		assumeThat(redisTemplate instanceof StringRedisTemplate).isTrue();

		List<Object> results = redisTemplate.executePipelined((RedisCallback) connection -> {
			StringRedisConnection stringRedisConn = (StringRedisConnection) connection;
			stringRedisConn.set("foo", "5");
			stringRedisConn.get("foo");
			stringRedisConn.rPush("foolist", "10");
			stringRedisConn.rPush("foolist", "11");
			stringRedisConn.lRange("foolist", 0, -1);
			return null;
		}, new GenericToStringSerializer<>(Long.class));

		assertThat(results).containsExactly(true, 5L, 1L, 2L, Arrays.asList(10L, 11L));
	}

	@ParameterizedRedisTest // DATAREDIS-500
	void testExecutePipelinedWidthDifferentHashKeySerializerAndHashValueSerializer() {

		assumeThat(redisTemplate instanceof StringRedisTemplate).isTrue();

		redisTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		redisTemplate.setHashKeySerializer(new GenericToStringSerializer<>(Long.class));
		redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<>(Person.class));

		Person person = new Person("Homer", "Simpson", 38);

		redisTemplate.opsForHash().put((K) "foo", 1L, person);

		List<Object> results = redisTemplate.executePipelined((RedisCallback) connection -> {
			connection.hGetAll(((StringRedisSerializer) redisTemplate.getKeySerializer()).serialize("foo"));
			return null;
		});

		assertThat(person).isEqualTo(((Map) results.get(0)).get(1L));
	}

	@ParameterizedRedisTest
	public void testExecutePipelinedNonNullRedisCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> redisTemplate.executePipelined((RedisCallback<String>) connection -> "Hey There"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@ParameterizedRedisTest
	public void testExecutePipelinedTx() {

		assumeThat(redisTemplate.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		List<Object> pipelinedResults = redisTemplate.executePipelined(new SessionCallback() {
			public Object execute(RedisOperations operations) throws DataAccessException {
				operations.multi();
				operations.opsForList().leftPush(key1, value1);
				operations.opsForList().rightPop(key1);
				operations.opsForList().size(key1);
				operations.exec();

				try {
					// Await EXEC completion as it's executed on a dedicated connection.
					Thread.sleep(100);
				} catch (InterruptedException e) {}
				operations.opsForValue().set(key1, value1);
				operations.opsForValue().get(key1);
				return null;
			}
		});
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertThat(pipelinedResults).usingElementComparator(CollectionAwareComparator.INSTANCE)
				.containsExactly(Arrays.asList(1L, value1, 0L), true, value1);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@ParameterizedRedisTest
	void testExecutePipelinedTxCustomSerializer() {
		assumeThat(redisTemplate.getConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);
		assumeThat(redisTemplate instanceof StringRedisTemplate).isTrue();
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
		}, new GenericToStringSerializer<>(Long.class));
		// Should contain the List of deserialized exec results and the result of the last call to get()
		assertThat(pipelinedResults).isEqualTo(Arrays.asList(Arrays.asList(1L, 5L, 0L), true, 2L));
	}

	@ParameterizedRedisTest
	public void testExecutePipelinedNonNullSessionCallback() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> redisTemplate.executePipelined(new SessionCallback<String>() {
					@SuppressWarnings("rawtypes")
					public String execute(RedisOperations operations) throws DataAccessException {
						return "Whatup";
					}
				}));
	}

	@ParameterizedRedisTest // DATAREDIS-688
	void testDelete() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		redisTemplate.opsForValue().set(key1, value1);

		assertThat(redisTemplate.hasKey(key1)).isTrue();
		assertThat(redisTemplate.delete(key1)).isTrue();
		assertThat(redisTemplate.hasKey(key1)).isFalse();
	}

	@ParameterizedRedisTest
	void testCopy() {

		assumeThat(redisTemplate.execute((RedisCallback<RedisConnection>) it -> it))
				.isNotInstanceOf(RedisClusterConnection.class);

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		redisTemplate.opsForValue().set(key1, value1);

		redisTemplate.copy(key1, key2, false);
		assertThat(redisTemplate.opsForValue().get(key2)).isEqualTo(value1);

		redisTemplate.opsForValue().set(key1, value2);

		redisTemplate.copy(key1, key2, true);
		assertThat(redisTemplate.opsForValue().get(key2)).isEqualTo(value2);
	}

	@ParameterizedRedisTest // DATAREDIS-688
	void testDeleteMultiple() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.opsForValue().set(key2, value2);

		assertThat(redisTemplate.delete(Arrays.asList(key1, key2)).longValue()).isEqualTo(2L);
		assertThat(redisTemplate.hasKey(key1)).isFalse();
		assertThat(redisTemplate.hasKey(key2)).isFalse();
	}

	@ParameterizedRedisTest
	void testSort() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		assumeThat(value1 instanceof Number).isTrue();

		redisTemplate.opsForList().rightPush(key1, value1);

		List<V> results = redisTemplate.sort(SortQueryBuilder.sort(key1).build());
		assertThat(results).isEqualTo(Collections.singletonList(value1));
	}

	@ParameterizedRedisTest
	void testSortStore() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeThat(value1 instanceof Number).isTrue();
		redisTemplate.opsForList().rightPush(key1, value1);
		assertThat(redisTemplate.sort(SortQueryBuilder.sort(key1).build(), key2)).isEqualTo(Long.valueOf(1));
		assertThat(redisTemplate.boundListOps(key2).range(0, -1)).isEqualTo(Collections.singletonList(value1));
	}

	@ParameterizedRedisTest
	public void testSortBulkMapper() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeThat(value1 instanceof Number).isTrue();
		redisTemplate.opsForList().rightPush(key1, value1);
		List<String> results = redisTemplate.sort(SortQueryBuilder.sort(key1).get("#").build(), tuple -> "FOO");
		assertThat(results).isEqualTo(Collections.singletonList("FOO"));
	}

	@ParameterizedRedisTest
	void testExpireAndGetExpireMillis() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 500, TimeUnit.MILLISECONDS);

		assertThat(redisTemplate.getExpire(key1, TimeUnit.MILLISECONDS)).isGreaterThan(0L);
	}

	@ParameterizedRedisTest
	void testGetExpireNoTimeUnit() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 2, TimeUnit.SECONDS);
		Long expire = redisTemplate.getExpire(key1);
		// Default behavior is to return seconds
		assertThat(expire > 0L && expire <= 2L).isTrue();
	}

	@ParameterizedRedisTest
	void testGetExpireSeconds() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expire(key1, 1500, TimeUnit.MILLISECONDS);
		assertThat(redisTemplate.getExpire(key1, TimeUnit.SECONDS)).isEqualTo(Long.valueOf(1));
	}

	@ParameterizedRedisTest // DATAREDIS-526
	void testGetExpireSecondsForKeyDoesNotExist() {

		Long expire = redisTemplate.getExpire(keyFactory.instance(), TimeUnit.SECONDS);
		assertThat(expire).isLessThan(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	void testGetExpireSecondsForKeyExistButHasNoAssociatedExpire() {

		K key = keyFactory.instance();
		Long expire = redisTemplate.getExpire(key, TimeUnit.SECONDS);

		assertThat(expire).isLessThan(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	void testGetExpireMillisForKeyDoesNotExist() {

		Long expire = redisTemplate.getExpire(keyFactory.instance(), TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	void testGetExpireMillisForKeyExistButHasNoAssociatedExpire() {

		K key = keyFactory.instance();
		redisTemplate.boundValueOps(key).set(valueFactory.instance());

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	void testGetExpireMillis() {

		K key = keyFactory.instance();
		redisTemplate.boundValueOps(key).set(valueFactory.instance());
		redisTemplate.expire(key, 1, TimeUnit.DAYS);

		Long ttl = redisTemplate.getExpire(key, TimeUnit.HOURS);

		assertThat(ttl).isGreaterThanOrEqualTo(23L);
		assertThat(ttl).isLessThan(25L);
	}

	@ParameterizedRedisTest // DATAREDIS-611
	void testGetExpireDuration() {

		K key = keyFactory.instance();
		redisTemplate.boundValueOps(key).set(valueFactory.instance());
		redisTemplate.expire(key, Duration.ofDays(1));

		Long ttl = redisTemplate.getExpire(key, TimeUnit.HOURS);

		assertThat(ttl).isGreaterThanOrEqualTo(23L);
		assertThat(ttl).isLessThan(25L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testGetExpireMillisUsingTransactions() {

		K key = keyFactory.instance();
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

		assertThat(result).hasSize(3);
		assertThat(((Long) result.get(2))).isGreaterThanOrEqualTo(23L);
		assertThat(((Long) result.get(2))).isLessThan(25L);
	}

	@ParameterizedRedisTest // DATAREDIS-526
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testGetExpireMillisUsingPipelining() {

		K key = keyFactory.instance();
		List<Object> result = redisTemplate.executePipelined(new SessionCallback<Object>() {

			@Override
			public Object execute(RedisOperations operations) throws DataAccessException {

				operations.boundValueOps(key).set(valueFactory.instance());
				operations.expire(key, 1, TimeUnit.DAYS);
				operations.getExpire(key, TimeUnit.HOURS);

				return null;
			}
		});

		assertThat(result).hasSize(3);
		assertThat(((Long) result.get(2))).isGreaterThanOrEqualTo(23L);
		assertThat(((Long) result.get(2))).isLessThan(25L);
	}

	@ParameterizedRedisTest
	void testExpireAt() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expireAt(key1, new Date(System.currentTimeMillis() + 5L));
		await().until(() -> !redisTemplate.hasKey(key1));
	}

	@ParameterizedRedisTest // DATAREDIS-611
	void testExpireAtInstant() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.boundValueOps(key1).set(value1);
		redisTemplate.expireAt(key1, Instant.now().plus(5, ChronoUnit.MILLIS));
		await().until(() -> !redisTemplate.hasKey(key1));
	}

	@ParameterizedRedisTest
	@EnabledIfLongRunningTest
	void testExpireAtMillisNotSupported() {

		assumeThat(redisTemplate.getConnectionFactory() instanceof JedisConnectionFactory).isTrue();

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		assumeThat(key1 instanceof String && value1 instanceof String).isTrue();

		StringRedisTemplate template2 = new StringRedisTemplate(redisTemplate.getConnectionFactory());
		template2.boundValueOps((String) key1).set((String) value1);
		template2.expireAt((String) key1, new Date(System.currentTimeMillis() + 5L));
		// Just ensure this works as expected, pExpireAt just adds some precision over expireAt
		await().until(() -> !template2.hasKey((String) key1));
	}

	@ParameterizedRedisTest
	void testPersist() throws Exception {
		// Test is meaningless in Redis 2.4 because key won't expire after 10 ms
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.expire(key1, 10, TimeUnit.MILLISECONDS);
		redisTemplate.persist(key1);
		Thread.sleep(10);
		assertThat(redisTemplate.hasKey(key1)).isTrue();
	}

	@ParameterizedRedisTest
	void testRandomKey() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		assertThat(redisTemplate.randomKey()).isEqualTo(key1);
	}

	@ParameterizedRedisTest
	void testRename() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.rename(key1, key2);
		assertThat(redisTemplate.opsForValue().get(key2)).isEqualTo(value1);
	}

	@ParameterizedRedisTest
	void testRenameIfAbsent() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		redisTemplate.renameIfAbsent(key1, key2);
		assertThat(redisTemplate.hasKey(key2)).isTrue();
	}

	@ParameterizedRedisTest
	void testType() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		assertThat(redisTemplate.type(key1)).isEqualTo(DataType.STRING);
	}

	@ParameterizedRedisTest // DATAREDIS-506
	public void testWatch() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);

		Thread th = new Thread(() -> redisTemplate.opsForValue().set(key1, value2));

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

		assertThat(results).isEmpty();

		assertThat(redisTemplate.opsForValue().get(key1)).isEqualTo(value2);
	}

	@ParameterizedRedisTest
	public void testUnwatch() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);
		Thread th = new Thread(() -> redisTemplate.opsForValue().set(key1, value2));

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

		assertThat(results.size()).isEqualTo(1);
		assertThat(redisTemplate.opsForValue().get(key1)).isEqualTo(value3);
	}

	@ParameterizedRedisTest // DATAREDIS-506
	public void testWatchMultipleKeys() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		redisTemplate.opsForValue().set(key1, value1);

		Thread th = new Thread(() -> redisTemplate.opsForValue().set(key1, value2));

		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public List<Object> execute(RedisOperations operations) throws DataAccessException {

				List<K> keys = new ArrayList<>();
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

		assertThat(results).isEmpty();

		assertThat(redisTemplate.opsForValue().get(key1)).isEqualTo(value2);
	}

	@ParameterizedRedisTest
	public void testConvertAndSend() {
		V value1 = valueFactory.instance();
		// Make sure basic message sent without Exception on serialization
		redisTemplate.convertAndSend("Channel", value1);
	}

	@ParameterizedRedisTest
	void testExecuteScriptCustomSerializers() {
		K key1 = keyFactory.instance();
		DefaultRedisScript<String> script = new DefaultRedisScript<>();
		script.setScriptText("return 'Hey'");
		script.setResultType(String.class);
		assertThat(redisTemplate.execute(script, redisTemplate.getValueSerializer(), StringRedisSerializer.UTF_8,
				Collections.singletonList(key1))).isEqualTo("Hey");
	}

	@ParameterizedRedisTest
	void clientListShouldReturnCorrectly() {
		assertThat(redisTemplate.getClientList().size()).isNotEqualTo(0);
	}

	@ParameterizedRedisTest // DATAREDIS-529
	void countExistingKeysReturnsNumberOfKeysCorrectly() {

		Map<K, V> source = new LinkedHashMap<>(3, 1);
		source.put(keyFactory.instance(), valueFactory.instance());
		source.put(keyFactory.instance(), valueFactory.instance());
		source.put(keyFactory.instance(), valueFactory.instance());

		redisTemplate.opsForValue().multiSet(source);

		assertThat(redisTemplate.countExistingKeys(source.keySet())).isEqualTo(3L);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private byte[] serialize(Object value, RedisSerializer serializer) {
		if (serializer == null && value instanceof byte[]) {
			return (byte[]) value;
		}
		return serializer.serialize(value);
	}
}
