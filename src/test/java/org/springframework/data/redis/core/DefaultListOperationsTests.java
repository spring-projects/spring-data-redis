/*
 * Copyright 2013-2019 the original author or authors.
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
import static org.junit.Assume.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.StringObjectFactory;

/**
 * Integration test of {@link DefaultListOperations}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @param <K> Key test
 * @param <V> Value test
 */
@RunWith(Parameterized.class)
public class DefaultListOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ListOperations<K, V> listOps;

	public DefaultListOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
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
		listOps = redisTemplate.opsForList();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test
	public void testLeftPushWithPivot() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.leftPush(key, v1)).isEqualTo(Long.valueOf(1));
		assertThat(listOps.leftPush(key, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.leftPush(key, v1, v3)).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v2, v3, v1);
	}

	@Test
	public void testLeftPushIfPresent() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(listOps.leftPushIfPresent(key, v1)).isEqualTo(Long.valueOf(0));
		assertThat(listOps.leftPush(key, v1)).isEqualTo(Long.valueOf(1));
		assertThat(listOps.leftPushIfPresent(key, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.range(key, 0, -1)).contains(v2, v1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testLeftPushAll() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.leftPushAll(key, v1, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.leftPush(key, v3)).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).contains(v3, v2, v1);
	}

	@Test
	public void testLeftPopDuration() {
		// 1 ms timeout gets upgraded to 1 sec timeout at the moment
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		assumeTrue(valueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(listOps.leftPop(key, Duration.ofSeconds(1))).isNull();
		listOps.rightPushAll(key, v1, v2);
		assertThat(listOps.leftPop(key, Duration.ofSeconds(1))).isEqualTo(v1);
	}

	@Test
	public void testRightPopDuration() {
		// 1 ms timeout gets upgraded to 1 sec timeout at the moment
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		assumeTrue(valueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(listOps.rightPop(key, Duration.ofSeconds(1))).isNull();
		listOps.rightPushAll(key, v1, v2);
		assertThat(listOps.rightPop(key, Duration.ofSeconds(1))).isEqualTo(v2);
	}

	@Test
	public void testRightPopAndLeftPushTimeout() {
		// 1 ms timeout gets upgraded to 1 sec timeout at the moment
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		assumeTrue(valueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();

		assertThat(listOps.rightPopAndLeftPush(key, key2, 1, TimeUnit.MILLISECONDS)).isNull();
		listOps.leftPush(key, v1);
		assertThat(listOps.rightPopAndLeftPush(key, key2, 1, TimeUnit.MILLISECONDS)).isEqualTo(v1);
	}

	@Test // DATAREDIS-611
	public void testRightPopAndLeftPushDuration() {
		// 1 ms timeout gets upgraded to 1 sec timeout at the moment
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		assumeTrue(valueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();

		assertThat(listOps.rightPopAndLeftPush(key, key2, Duration.ofMillis(1))).isNull();
		listOps.leftPush(key, v1);
		assertThat(listOps.rightPopAndLeftPush(key, key2, Duration.ofMillis(1))).isEqualTo(v1);
	}

	@Test
	public void testRightPopAndLeftPush() {

		K key = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();

		assertThat(listOps.rightPopAndLeftPush(key, key2)).isNull();
		listOps.leftPush(key, v1);
		assertThat(listOps.rightPopAndLeftPush(key, key2)).isEqualTo(v1);
	}

	@Test
	public void testRightPushWithPivot() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.rightPush(key, v1)).isEqualTo(Long.valueOf(1));
		assertThat(listOps.rightPush(key, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.rightPush(key, v1, v3)).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v1, v3, v2);
	}

	@Test
	public void testRightPushIfPresent() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(listOps.rightPushIfPresent(key, v1)).isEqualTo(Long.valueOf(0));
		assertThat(listOps.rightPush(key, v1)).isEqualTo(Long.valueOf(1));
		assertThat(listOps.rightPushIfPresent(key, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v1, v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRightPushAll() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.rightPushAll(key, v1, v2)).isEqualTo(Long.valueOf(2));
		assertThat(listOps.rightPush(key, v3)).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v1, v2, v3);
	}

	@Test // DATAREDIS-288
	@SuppressWarnings("unchecked")
	public void testRightPushAllCollection() {

		K key = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.rightPushAll(key, Arrays.<V> asList(v1, v2, v3))).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v1, v2, v3);
	}

	@Test // DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCalledWithEmptyCollection() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.rightPushAll(keyFactory.instance(), Collections.<V> emptyList()));
	}

	@Test
	@SuppressWarnings("unchecked")
	// DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCollectionContainsNullValue() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.rightPushAll(keyFactory.instance(), Arrays.asList(valueFactory.instance(), null)));
	}

	@Test // DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCalledWithNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.rightPushAll(keyFactory.instance(), (Collection<V>) null));
	}

	@Test // DATAREDIS-288
	@SuppressWarnings("unchecked")
	public void testLeftPushAllCollection() {

		K key = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertThat(listOps.leftPushAll(key, Arrays.<V> asList(v1, v2, v3))).isEqualTo(Long.valueOf(3));
		assertThat(listOps.range(key, 0, -1)).containsSequence(v3, v2, v1);
	}

	@Test // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCalledWithEmptyCollection() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.leftPushAll(keyFactory.instance(), Collections.<V> emptyList()));
	}

	@Test // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCollectionContainsNullValue() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.leftPushAll(keyFactory.instance(), Arrays.asList(valueFactory.instance(), null)));
	}

	@Test // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCalledWithNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> listOps.leftPushAll(keyFactory.instance(), (Collection<V>) null));
	}
}
