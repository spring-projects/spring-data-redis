/*
 * Copyright 2013-2021 the original author or authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration test of {@link DefaultHashOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @param <K> Key type
 * @param <HK> Hash key type
 * @param <HV> Hash value type
 */
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
				.getConnectionFactory(RedisStanalone.class);

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

	@ParameterizedRedisTest
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

	@ParameterizedRedisTest
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

	@ParameterizedRedisTest // DATAREDIS-305
	void testHScanReadsValuesFully() throws IOException {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		Cursor<Map.Entry<HK, HV>> it = hashOps.scan(key, ScanOptions.scanOptions().count(1).build());

		long count = 0;
		while (it.hasNext()) {
			Map.Entry<HK, HV> entry = it.next();
			assertThat(entry.getKey()).isIn(key1, key2);
			assertThat(entry.getValue()).isIn(val1, val2);
			count++;
		}

		it.close();
		assertThat(count).isEqualTo(hashOps.size(key));
	}

	@ParameterizedRedisTest // DATAREDIS-698
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

	@ParameterizedRedisTest // GH-2048
	void randomField() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		assertThat(hashOps.randomField(key)).isIn(key1, key2);
		assertThat(hashOps.randomFields(key, 2)).hasSize(2).contains(key1, key2);
	}

	@ParameterizedRedisTest // GH-2048
	void randomValue() {

		assumeThat(hashKeyFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);

		Map.Entry<HK, HV> entry = hashOps.randomValue(key);

		if (entry.getKey().equals(key1)) {
			assertThat(entry.getValue()).isEqualTo(val1);
		} else {
			assertThat(entry.getValue()).isEqualTo(val2);
		}

		Map<HK, HV> values = hashOps.randomValues(key, 10);
		assertThat(values).hasSize(2).containsEntry(key1, val1).containsEntry(key2, val2);
	}
}
