/*
 * Copyright 2013-2020 the original author or authors.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link DefaultSetOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultSetOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private SetOperations<K, V> setOps;

	public @Rule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	public DefaultSetOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
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
		setOps = redisTemplate.opsForSet();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDistinctRandomMembers() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(setKey, v1);
		setOps.add(setKey, v2);
		setOps.add(setKey, v3);

		Set<V> members = setOps.distinctRandomMembers(setKey, 2);
		assertThat(members).hasSize(2).contains(v1, v2, v3);
	}

	@Test
	public void testRandomMembersWithDuplicates() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		setOps.add(setKey, v1);
		setOps.add(setKey, v2);

		List<V> members = setOps.randomMembers(setKey, 2);
		assertThat(members).hasSize(2).contains(v1, v2);
	}

	@Test
	public void testRandomMembersNegative() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		try {
			setOps.randomMembers(keyFactory.instance(), -1);
			fail("IllegalArgumentException should be thrown");
		} catch (IllegalArgumentException e) {}
	}

	@Test
	public void testDistinctRandomMembersNegative() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		try {
			setOps.distinctRandomMembers(keyFactory.instance(), -2);
			fail("IllegalArgumentException should be thrown");
		} catch (IllegalArgumentException e) {}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMove() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		setOps.add(key1, v1);
		setOps.add(key1, v2);
		setOps.move(key1, v1, key2);

		assertThat(setOps.members(key1)).containsOnly(v2);
		assertThat(setOps.members(key2)).containsOnly(v1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPop() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		setOps.add(key, v1);

		assertThat(setOps.pop(key)).isEqualTo(v1);
		assertThat(setOps.members(key)).isEmpty();
	}

	@Test // DATAREDIS-668
	public void testPopWithCount() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);

		List<V> result = setOps.pop(key, 2);
		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isInstanceOf(v1.getClass());
	}

	@Test
	public void testRandomMember() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		setOps.add(key, v1);

		assertThat(setOps.randomMember(key)).isEqualTo(v1);
	}

	@Test
	public void testAdd() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(setOps.add(key, v1, v2)).isEqualTo(Long.valueOf(2));
		assertThat(setOps.members(key)).containsOnly(v1, v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemove() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);
		assertThat(setOps.remove(key, v1, v2, v4)).isEqualTo(Long.valueOf(2));

		assertThat(setOps.members(key)).containsOnly(v3);
	}

	@Test // DATAREDIS-304
	@SuppressWarnings("unchecked")
	@IfProfileValue(name = "redisVersion", value = "2.8+")
	public void testSSCanReadsValuesFully() throws IOException {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);
		Cursor<V> it = setOps.scan(key, ScanOptions.scanOptions().count(1).build());
		long count = 0;
		while (it.hasNext()) {
			assertThat(it.next()).isIn(v1, v2, v3);
			count++;
		}
		it.close();
		assertThat(count).isEqualTo(setOps.size(key));
	}

	@Test // DATAREDIS-873
	public void diffShouldReturnDifference() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.difference(Arrays.asList(sourceKey1, sourceKey2))).contains(v1);
	}

	@Test // DATAREDIS-873
	public void diffAndStoreShouldReturnDifferenceShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.differenceAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(1L);
	}

	@Test // DATAREDIS-873
	public void unionShouldConcatSets() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.union(Arrays.asList(sourceKey1, sourceKey2))).contains(v1, v2, v3, v4);
	}

	@Test // DATAREDIS-873
	public void unionAndStoreShouldReturnDifferenceShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.unionAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(4L);
	}

	@Test // DATAREDIS-873
	public void intersectShouldReturnElements() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.intersect(Arrays.asList(sourceKey1, sourceKey2))).hasSize(2);
	}

	@Test // DATAREDIS-448, DATAREDIS-873
	public void intersectAndStoreShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.intersectAndStore(sourceKey1, sourceKey2, destinationKey)).isEqualTo(2L);
		assertThat(setOps.intersectAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(2L);
	}
}
