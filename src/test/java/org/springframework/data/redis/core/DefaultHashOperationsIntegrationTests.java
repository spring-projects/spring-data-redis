/*
 * Copyright 2013-2025 the original author or authors.
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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.ExpireChanges.ExpiryChangeState;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations.TimeToLive;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test of {@link DefaultHashOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Tihomir Mateev
 * @param <K> Key type
 * @param <HK> Hash key type
 * @param <HV> Hash value type
 */
@ParameterizedClass
@MethodSource("testParams")
public class DefaultHashOperationsIntegrationTests<K, HK, HV> {

	private final RedisTemplate<K, ?> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;
	private final HashOperations<K, HK, HV> hashOps;

	public DefaultHashOperationsIntegrationTests(RedisTemplate<K, ?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;
		this.hashOps = redisTemplate.opsForHash();
	}

	public static Collection<Object[]> testParams() {
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(jedisConnectionFactory);
		stringTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setConnectionFactory(jedisConnectionFactory);
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory, stringFactory },
				{ rawTemplate, rawFactory, rawFactory, rawFactory } });
	}

	@BeforeEach
	void setUp() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test
	void testEntries() {
		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		for (Map.Entry<HK, HV> entry : hashOps.entries(key).entrySet()) {
			assertThat(entry.getKey()).isIn(key1, key2);
			assertThat(entry.getValue()).isIn(val1, val2);
		}
	}

	@Test
	void testDelete() {
		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);
		Long numDeleted = hashOps.delete(key, key1, key2);
		assertThat(hashOps.keys(key).isEmpty()).isTrue();
		assertThat(numDeleted.longValue()).isEqualTo(2L);
	}

	@Test // DATAREDIS-305
	void testHScanReadsValuesFully() throws IOException {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		long count = 0;
		try (Cursor<Map.Entry<HK, HV>> it = hashOps.scan(key, ScanOptions.scanOptions().count(1).build())) {

			while (it.hasNext()) {
				Map.Entry<HK, HV> entry = it.next();
				assertThat(entry.getKey()).isIn(key1, key2);
				assertThat(entry.getValue()).isIn(val1, val2);
				count++;
			}
		}

		assertThat(count).isEqualTo(hashOps.size(key));
	}

	@Test // DATAREDIS-698
	void lengthOfValue() throws IOException {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(hashOps.lengthOfValue(key, key1)).isEqualTo(Long.valueOf(val1.toString().length()));
	}

	@Test // GH-2048
	void randomField() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(hashOps.randomKey(key)).isIn(key1, key2);
		assertThat(hashOps.randomKeys(key, 2)).hasSize(2).contains(key1, key2);
	}

	@Test // GH-2048
	void randomValue() {

		assumeThat(hashKeyFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		Map.Entry<HK, HV> entry = hashOps.randomEntry(key);

		if (entry.getKey().equals(key1)) {
			assertThat(entry.getValue()).isEqualTo(val1);
		} else {
			assertThat(entry.getValue()).isEqualTo(val2);
		}

		Map<HK, HV> values = hashOps.randomEntries(key, 10);
		assertThat(values).hasSize(2).containsEntry(key1, val1).containsEntry(key2, val2);
	}

	@EnabledOnCommand("HEXPIRE") // GH-3054
	@Test
	void testExpireAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(redisTemplate.opsForHash().expire(key, Duration.ofMillis(500), List.of(key1)))
				.satisfies(ExpireChanges::allOk);

		assertThat(redisTemplate.opsForHash().getTimeToLive(key, List.of(key1))).satisfies(expirations -> {

			assertThat(expirations.missing()).isEmpty();
			assertThat(expirations.timeUnit()).isEqualTo(TimeUnit.SECONDS);
			assertThat(expirations.expirationOf(key1)).extracting(TimeToLive::raw, InstanceOfAssertFactories.LONG)
					.isBetween(0L, 1L);
			assertThat(expirations.ttlOf(key1)).isBetween(Duration.ZERO, Duration.ofSeconds(1));
		});
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireAndGetExpireSeconds() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(redisTemplate.opsForHash().expire(key, Duration.ofSeconds(5), List.of(key1, key2)))
				.satisfies(changes -> {
					assertThat(changes.allOk()).isTrue();
					assertThat(changes.stateOf(key1)).isEqualTo(ExpiryChangeState.OK);
					assertThat(changes.ok()).containsExactlyInAnyOrder(key1, key2);
					assertThat(changes.missed()).isEmpty();
					assertThat(changes.stateChanges()).map(ExpiryChangeState::value).containsExactly(1L, 1L);
				});

		assertThat(redisTemplate.opsForHash().getTimeToLive(key, TimeUnit.SECONDS, List.of(key1, key2)))
				.satisfies(expirations -> {
					assertThat(expirations.missing()).isEmpty();
					assertThat(expirations.timeUnit()).isEqualTo(TimeUnit.SECONDS);
					assertThat(expirations.expirationOf(key1)).extracting(TimeToLive::raw, InstanceOfAssertFactories.LONG)
							.isBetween(0L, 5L);
					assertThat(expirations.ttlOf(key1)).isBetween(Duration.ofSeconds(1), Duration.ofSeconds(5));
				});
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testBoundExpireAndGetExpireSeconds() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		BoundHashOperations<K, HK, HV> hashOps = redisTemplate.boundHashOps(key);
		BoundHashFieldExpirationOperations<HK> exp = hashOps.hashExpiration(key1, key2);

		assertThat(exp.expire(Duration.ofSeconds(5))).satisfies(changes -> {
			assertThat(changes.allOk()).isTrue();
			assertThat(changes.stateOf(key1)).isEqualTo(ExpiryChangeState.OK);
			assertThat(changes.ok()).containsExactlyInAnyOrder(key1, key2);
			assertThat(changes.missed()).isEmpty();
			assertThat(changes.stateChanges()).map(ExpiryChangeState::value).containsExactly(1L, 1L);
		});

		assertThat(exp.getTimeToLive(TimeUnit.SECONDS)).satisfies(expirations -> {
			assertThat(expirations.missing()).isEmpty();
			assertThat(expirations.timeUnit()).isEqualTo(TimeUnit.SECONDS);
			assertThat(expirations.expirationOf(key1)).extracting(TimeToLive::raw, InstanceOfAssertFactories.LONG)
					.isBetween(0L, 5L);
			assertThat(expirations.ttlOf(key1)).isBetween(Duration.ofSeconds(1), Duration.ofSeconds(5));
		});
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	void testBoundHashOperationsGetAndExpire() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();

		// Set up test data
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);
		hashOps.put(key, key3, val3);

		BoundHashOperations<K, HK, HV> boundHashOps = redisTemplate.boundHashOps(key);

		// Test single field get and expire
		List<HV> result1 = boundHashOps.getAndExpire(Expiration.seconds(60), Arrays.asList(key1));
		assertThat(result1).hasSize(1).containsExactly(val1);

		// Verify field still exists but has expiration
		assertThat(boundHashOps.hasKey(key1)).isTrue();
		assertThat(boundHashOps.get(key1)).isEqualTo(val1);

		// Test multiple fields get and expire
		List<HV> result2 = boundHashOps.getAndExpire(Expiration.seconds(120), Arrays.asList(key2, key3));
		assertThat(result2).hasSize(2).containsExactly(val2, val3);

		// Verify fields still exist but have expiration
		assertThat(boundHashOps.hasKey(key2)).isTrue();
		assertThat(boundHashOps.hasKey(key3)).isTrue();
		assertThat(boundHashOps.get(key2)).isEqualTo(val2);
		assertThat(boundHashOps.get(key3)).isEqualTo(val3);

		// Test non-existent field
		HK nonExistentKey = hashKeyFactory.instance();
		List<HV> result3 = boundHashOps.getAndExpire(Expiration.seconds(60), Arrays.asList(nonExistentKey));
		assertThat(result3).hasSize(1).containsExactly((HV) null);

		// Test empty fields collection
		List<HV> result4 = boundHashOps.getAndExpire(Expiration.seconds(60), Collections.emptyList());
		assertThat(result4).isEmpty();
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireAtAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(redisTemplate.opsForHash().expireAt(key, Instant.now().plusMillis(500), List.of(key1, key2)))
				.satisfies(ExpireChanges::allOk);

		assertThat(redisTemplate.opsForHash().getTimeToLive(key, TimeUnit.MILLISECONDS, List.of(key1, key2)))
				.satisfies(expirations -> {
					assertThat(expirations.missing()).isEmpty();
					assertThat(expirations.timeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
					assertThat(expirations.expirationOf(key1)).extracting(TimeToLive::raw, InstanceOfAssertFactories.LONG)
							.isBetween(0L, 500L);
					assertThat(expirations.ttlOf(key1)).isBetween(Duration.ZERO, Duration.ofMillis(500));
				});
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void expireThrowsErrorOfNanoPrecision() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> redisTemplate.opsForHash().getTimeToLive(key, TimeUnit.NANOSECONDS, List.of(key1)));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireWithOptionsNone() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		ExpireChanges<Object> expire = redisTemplate.opsForHash().expire(key,
				org.springframework.data.redis.core.types.Expiration.seconds(20), ExpirationOptions.none(), List.of(key1));

		assertThat(expire.allOk()).isTrue();
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireWithOptions() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		redisTemplate.opsForHash().expire(key, org.springframework.data.redis.core.types.Expiration.seconds(20),
				ExpirationOptions.none(), List.of(key1));
		redisTemplate.opsForHash().expire(key, org.springframework.data.redis.core.types.Expiration.seconds(60),
				ExpirationOptions.none(), List.of(key2));

		ExpireChanges<Object> changes = redisTemplate.opsForHash().expire(key,
				org.springframework.data.redis.core.types.Expiration.seconds(30), ExpirationOptions.builder().gt().build(),
				List.of(key1, key2));

		assertThat(changes.ok()).containsExactly(key1);
		assertThat(changes.skipped()).containsExactly(key2);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testPersistAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(redisTemplate.opsForHash().expireAt(key, Instant.now().plusMillis(800), List.of(key1, key2)))
				.satisfies(ExpireChanges::allOk);

		assertThat(redisTemplate.opsForHash().persist(key, List.of(key2))).satisfies(ExpireChanges::allOk);

		assertThat(redisTemplate.opsForHash().getTimeToLive(key, List.of(key1, key2))).satisfies(expirations -> {
			assertThat(expirations.expirationOf(key1).isPersistent()).isFalse();
			assertThat(expirations.expirationOf(key2).isPersistent()).isTrue();
		});
	}

    @Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void testGetAndDelete() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();

		// Set up test data
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);
		hashOps.put(key, key3, val3);

		// Test single field get and delete
		List<HV> result = hashOps.getAndDelete(key, List.of(key1));
		assertThat(result).hasSize(1).containsExactly(val1);
		assertThat(hashOps.hasKey(key, key1)).isFalse(); // Field should be deleted
		assertThat(hashOps.hasKey(key, key2)).isTrue();  // Other fields should remain

		// Test multiple fields get and delete
		List<HV> multiResult = hashOps.getAndDelete(key, List.of(key2, key3));
		assertThat(multiResult).hasSize(2).containsExactly(val2, val3);
		assertThat(hashOps.hasKey(key, key2)).isFalse(); // Both fields should be deleted
		assertThat(hashOps.hasKey(key, key3)).isFalse();
		assertThat(hashOps.size(key)).isEqualTo(0L); // Hash should be empty

		// Test get and delete on non-existent field
		HK nonExistentKey = hashKeyFactory.instance();
		List<HV> emptyResult = hashOps.getAndDelete(key, List.of(nonExistentKey));
		assertThat(emptyResult).hasSize(1);
		assertThat(emptyResult.get(0)).isNull();

		// Test get and delete on non-existent hash
		K nonExistentHash = keyFactory.instance();
		List<HV> nonExistentHashResult = hashOps.getAndDelete(nonExistentHash, List.of(key1));
		assertThat(nonExistentHashResult).hasSize(1);
		assertThat(nonExistentHashResult.get(0)).isNull();

		// Test that key is deleted when all fields are removed
		K keyForDeletion = keyFactory.instance();
		HK field1 = hashKeyFactory.instance();
		HK field2 = hashKeyFactory.instance();
		HV value1 = hashValueFactory.instance();
		HV value2 = hashValueFactory.instance();

		// Set up hash with two fields
		hashOps.put(keyForDeletion, field1, value1);
		hashOps.put(keyForDeletion, field2, value2);
		assertThat(redisTemplate.hasKey(keyForDeletion)).isTrue(); // Key should exist

		// Delete all fields at once - key should be deleted
		List<HV> allFieldsResult = hashOps.getAndDelete(keyForDeletion, List.of(field1, field2));
		assertThat(allFieldsResult).hasSize(2).containsExactly(value1, value2);
		assertThat(redisTemplate.hasKey(keyForDeletion)).isFalse(); // Key should be deleted when last field is removed
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	void testGetAndExpire() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();

		// Set up test data
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);
		hashOps.put(key, key3, val3);

		// Test single field get and expire
		List<HV> result1 = hashOps.getAndExpire(key, Expiration.seconds(60), Arrays.asList(key1));
		assertThat(result1).hasSize(1).containsExactly(val1);

		// Verify field still exists but has expiration
		assertThat(hashOps.hasKey(key, key1)).isTrue();
		assertThat(hashOps.get(key, key1)).isEqualTo(val1);

		// Test multiple fields get and expire
		List<HV> result2 = hashOps.getAndExpire(key, Expiration.seconds(120), Arrays.asList(key2, key3));
		assertThat(result2).hasSize(2).containsExactly(val2, val3);

		// Verify fields still exist but have expiration
		assertThat(hashOps.hasKey(key, key2)).isTrue();
		assertThat(hashOps.hasKey(key, key3)).isTrue();
		assertThat(hashOps.get(key, key2)).isEqualTo(val2);
		assertThat(hashOps.get(key, key3)).isEqualTo(val3);

		// Test non-existent field
		HK nonExistentKey = hashKeyFactory.instance();
		List<HV> result3 = hashOps.getAndExpire(key, Expiration.seconds(60), Arrays.asList(nonExistentKey));
		assertThat(result3).hasSize(1).containsExactly((HV) null);

		// Test mixed existing and non-existent fields
		HK key4 = hashKeyFactory.instance();
		HV val4 = hashValueFactory.instance();
		hashOps.put(key, key4, val4);

		List<HV> result4 = hashOps.getAndExpire(key, Expiration.seconds(60), Arrays.asList(key4, nonExistentKey));
		assertThat(result4).hasSize(2);
		assertThat(result4.get(0)).isEqualTo(val4);
		assertThat(result4.get(1)).isNull();

		// Verify existing field still exists with expiration
		assertThat(hashOps.hasKey(key, key4)).isTrue();
		assertThat(hashOps.get(key, key4)).isEqualTo(val4);

		// Test empty fields collection
		List<HV> result5 = hashOps.getAndExpire(key, Expiration.seconds(60), Collections.emptyList());
		assertThat(result5).isEmpty();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void testPutAndExpire() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();

		// Test UPSERT condition - should always set fields
		Map<HK, HV> fieldMap1 = Map.of(key1, val1, key2, val2);
		Boolean result1 = hashOps.putAndExpire(key, fieldMap1, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60));
		assertThat(result1).isTrue();

		// Verify fields were set and exist
		assertThat(hashOps.hasKey(key, key1)).isTrue();
		assertThat(hashOps.hasKey(key, key2)).isTrue();
		assertThat(hashOps.get(key, key1)).isEqualTo(val1);
		assertThat(hashOps.get(key, key2)).isEqualTo(val2);

		// Test IF_NONE_EXIST condition - should not change existing fields
		Map<HK, HV> fieldMap2 = Map.of(key1, val3, key3, val3);
		Boolean result2 = hashOps.putAndExpire(key, fieldMap2, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(120));
		assertThat(result2).isFalse();

		// Verify original values unchanged (IF_NONE_EXIST failed because key1 exists)
		assertThat(hashOps.get(key, key1)).isEqualTo(val1);
		assertThat(hashOps.hasKey(key, key3)).isFalse();

		// Test IF_ALL_EXIST condition - should succeed because all fields exist
		Map<HK, HV> fieldMap3 = Map.of(key1, val3, key2, val3);
		Boolean result3 = hashOps.putAndExpire(key, fieldMap3, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(180));
		assertThat(result3).isTrue();

		// Verify values were updated
		assertThat(hashOps.get(key, key1)).isEqualTo(val3);
		assertThat(hashOps.get(key, key2)).isEqualTo(val3);

		// Test IF_ALL_EXIST condition with non-existent field - should not change anything
		HK nonExistentKey = hashKeyFactory.instance();
		Map<HK, HV> fieldMap4 = Map.of(key1, val1, nonExistentKey, val1);
		Boolean result4 = hashOps.putAndExpire(key, fieldMap4, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(60));
		assertThat(result4).isFalse();

		// Verify values unchanged (IF_ALL_EXIST failed because nonExistentKey doesn't exist)
		assertThat(hashOps.get(key, key1)).isEqualTo(val3);
		assertThat(hashOps.hasKey(key, nonExistentKey)).isFalse();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void testBoundHashOperationsPutAndExpire() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();

		BoundHashOperations<K, HK, HV> boundHashOps = redisTemplate.boundHashOps(key);

		// Test UPSERT condition - should always set fields
		Map<HK, HV> fieldMap1 = Map.of(key1, val1, key2, val2);
		boundHashOps.putAndExpire(fieldMap1, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60));

		// Verify fields were set and exist
		assertThat(boundHashOps.hasKey(key1)).isTrue();
		assertThat(boundHashOps.hasKey(key2)).isTrue();
		assertThat(boundHashOps.get(key1)).isEqualTo(val1);
		assertThat(boundHashOps.get(key2)).isEqualTo(val2);

		// Test IF_NONE_EXIST condition - should not change existing fields
		Map<HK, HV> fieldMap2 = Map.of(key1, val3, key3, val3);
		boundHashOps.putAndExpire(fieldMap2, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(120));

		// Verify original values unchanged (IF_NONE_EXIST failed because key1 exists)
		assertThat(boundHashOps.get(key1)).isEqualTo(val1);
		assertThat(boundHashOps.hasKey(key3)).isFalse();

		// Test IF_ALL_EXIST condition - should succeed because all fields exist
		Map<HK, HV> fieldMap3 = Map.of(key1, val3, key2, val3);
		boundHashOps.putAndExpire(fieldMap3, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(180));

		// Verify values were updated
		assertThat(boundHashOps.get(key1)).isEqualTo(val3);
		assertThat(boundHashOps.get(key2)).isEqualTo(val3);

		// Test IF_ALL_EXIST condition with non-existent field - should not change anything
		HK nonExistentKey = hashKeyFactory.instance();
		Map<HK, HV> fieldMap4 = Map.of(key1, val1, nonExistentKey, val1);
		boundHashOps.putAndExpire(fieldMap4, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(60));

		// Verify values unchanged (IF_ALL_EXIST failed because nonExistentKey doesn't exist)
		assertThat(boundHashOps.get(key1)).isEqualTo(val3);
		assertThat(boundHashOps.hasKey(nonExistentKey)).isFalse();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void testPutAndExpireWithDifferentExpirationPolicies() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		HK key3 = hashKeyFactory.instance();
		HV val3 = hashValueFactory.instance();
		HK key4 = hashKeyFactory.instance();
		HV val4 = hashValueFactory.instance();
		HK key5 = hashKeyFactory.instance();
		HV val5 = hashValueFactory.instance();

		// Test with seconds expiration
		Map<HK, HV> fieldMap1 = Map.of(key1, val1);
		Boolean result1 = hashOps.putAndExpire(key, fieldMap1, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60));
		assertThat(result1).isTrue();
		assertThat(hashOps.hasKey(key, key1)).isTrue();
		assertThat(hashOps.get(key, key1)).isEqualTo(val1);

		// Test with milliseconds expiration
		Map<HK, HV> fieldMap2 = Map.of(key2, val2);
		Boolean result2 = hashOps.putAndExpire(key, fieldMap2, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.milliseconds(120000));
		assertThat(result2).isTrue();
		assertThat(hashOps.hasKey(key, key2)).isTrue();
		assertThat(hashOps.get(key, key2)).isEqualTo(val2);

		// Test with Duration expiration
		Map<HK, HV> fieldMap3 = Map.of(key3, val3);
		Boolean result3 = hashOps.putAndExpire(key, fieldMap3, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.from(Duration.ofMinutes(3)));
		assertThat(result3).isTrue();
		assertThat(hashOps.hasKey(key, key3)).isTrue();
		assertThat(hashOps.get(key, key3)).isEqualTo(val3);

		// Test with unix timestamp expiration (5 minutes from now)
		long futureTimestamp = System.currentTimeMillis() / 1000 + 300; // 5 minutes from now
		Map<HK, HV> fieldMap4 = Map.of(key4, val4);
		Boolean result4 = hashOps.putAndExpire(key, fieldMap4, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.unixTimestamp(futureTimestamp, TimeUnit.SECONDS));
		assertThat(result4).isTrue();
		assertThat(hashOps.hasKey(key, key4)).isTrue();
		assertThat(hashOps.get(key, key4)).isEqualTo(val4);

		// Test with keepTtl expiration (should preserve existing TTL)
		// First set a field with TTL, then update it with keepTtl
		hashOps.put(key, key5, val5);
		hashOps.expire(key, Duration.ofMinutes(4), Arrays.asList(key5)); // Set initial TTL

		Map<HK, HV> fieldMap5 = Map.of(key5, val5);
		Boolean result5 = hashOps.putAndExpire(key, fieldMap5, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.keepTtl());
		assertThat(result5).isTrue();
		assertThat(hashOps.hasKey(key, key5)).isTrue();
		assertThat(hashOps.get(key, key5)).isEqualTo(val5);
		// TTL should be preserved (we can't easily test the exact value, but field should still exist)
	}
}
