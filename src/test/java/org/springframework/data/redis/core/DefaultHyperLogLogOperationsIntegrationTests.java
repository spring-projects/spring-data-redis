/*
 * Copyright 2014-present the original author or authors.
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

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.ObjectFactory;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ParameterizedClass
@MethodSource("testParams")
public class DefaultHyperLogLogOperationsIntegrationTests<K, V> {

	private final RedisTemplate<K, V> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;
	private final HyperLogLogOperations<K, V> hyperLogLogOps;

	public DefaultHyperLogLogOperationsIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.hyperLogLogOps = redisTemplate.opsForHyperLogLog();
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

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	void addShouldAddDistinctValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(hyperLogLogOps.add(key, v1, v2, v3)).isEqualTo(1L);
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	void addShouldNotAddExistingValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		assertThat(hyperLogLogOps.add(key, v2)).isEqualTo(0L);
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	void sizeShouldCountValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		assertThat(hyperLogLogOps.size(key)).isEqualTo(3L);
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	void sizeShouldCountValuesOfMultipleKeysCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		K key2 = keyFactory.instance();
		V v4 = valueFactory.instance();
		V v5 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		hyperLogLogOps.add(key2, v4, v5);
		assertThat(hyperLogLogOps.size(key, key2)).isGreaterThan(3L);
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	void unionShouldMergeValuesOfMultipleKeysCorrectly() throws InterruptedException {

		K sourceKey_1 = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		K sourceKey_2 = keyFactory.instance();
		V v4 = valueFactory.instance();
		V v5 = valueFactory.instance();

		K desinationKey = keyFactory.instance();

		hyperLogLogOps.add(sourceKey_1, v1, v2, v3);
		hyperLogLogOps.add(sourceKey_2, v4, v5);
		hyperLogLogOps.union(desinationKey, sourceKey_1, sourceKey_2);

		assertThat(hyperLogLogOps.size(desinationKey)).isGreaterThan(3L);
	}
}
