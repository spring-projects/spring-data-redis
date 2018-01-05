/*
 * Copyright 2014-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@IfProfileValue(name = "redisVersion", value = "2.8.9+")
public class DefaultHyperLogLogOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private HyperLogLogOperations<K, V> hyperLogLogOps;

	public static @ClassRule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	public DefaultHyperLogLogOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {
		hyperLogLogOps = redisTemplate.opsForHyperLogLog();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	public void addShouldAddDistinctValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(hyperLogLogOps.add(key, v1, v2, v3), equalTo(1L));
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	public void addShouldNotAddExistingValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		assertThat(hyperLogLogOps.add(key, v2), equalTo(0L));
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	public void sizeShouldCountValuesCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		assertThat(hyperLogLogOps.size(key), equalTo(3L));
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	public void sizeShouldCountValuesOfMultipleKeysCorrectly() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		K key2 = keyFactory.instance();
		V v4 = valueFactory.instance();

		hyperLogLogOps.add(key, v1, v2, v3);
		hyperLogLogOps.add(key2, v4);
		assertThat(hyperLogLogOps.size(key, key2), equalTo(4L));
	}

	@Test // DATAREDIS-308
	@SuppressWarnings("unchecked")
	public void unionShouldMergeValuesOfMultipleKeysCorrectly() throws InterruptedException {

		K sourceKey_1 = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		K sourceKey_2 = keyFactory.instance();
		V v4 = valueFactory.instance();

		K desinationKey = keyFactory.instance();

		hyperLogLogOps.add(sourceKey_1, v1, v2, v3);
		hyperLogLogOps.add(sourceKey_2, v4);

		Thread.sleep(10); // give redis a little time to catch up
		hyperLogLogOps.union(desinationKey, sourceKey_1, sourceKey_2);
		Thread.sleep(10); // give redis a little time to catch up

		assertThat(hyperLogLogOps.size(desinationKey), equalTo(4L));
	}
}
