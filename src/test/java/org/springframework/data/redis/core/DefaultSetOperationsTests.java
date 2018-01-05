/*
 * Copyright 2013-2018 the original author or authors.
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
import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hamcrest.CoreMatchers;
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
 */
@RunWith(Parameterized.class)
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
		assertEquals(2, members.size());
		Set<V> expected = new HashSet<>();
		expected.add(v1);
		expected.add(v2);
		expected.add(v3);
		assertThat(expected, hasItems((V[]) members.toArray()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRandomMembersWithDuplicates() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		setOps.add(setKey, v1);
		setOps.add(setKey, v2);
		List<V> members = setOps.randomMembers(setKey, 2);
		assertEquals(2, members.size());

		assertThat(members, CoreMatchers.<Iterable<? super V>> either(hasItem(v1)).or(hasItem(v2)));
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
		assertThat(setOps.members(key1), isEqual(new HashSet<>(Collections.singletonList(v2))));
		assertThat(setOps.members(key2), isEqual(new HashSet<>(Collections.singletonList(v1))));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPop() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		setOps.add(key, v1);
		assertThat(setOps.pop(key), isEqual(v1));
		assertTrue(setOps.members(key).isEmpty());
	}

	@Test // DATAREDIS-668
	public void testPopWithCount() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		setOps.add(key, v1, v2, v3);

		List<V> result = setOps.pop(key, 2);
		assertThat(result, hasSize(2));
		assertThat(result.get(0), instanceOf(v1.getClass()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRandomMember() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		setOps.add(key, v1);
		assertThat(setOps.randomMember(key), isEqual(v1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAdd() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		assertEquals(Long.valueOf(2), setOps.add(key, v1, v2));
		Set<V> expected = new HashSet<>();
		expected.add(v1);
		expected.add(v2);
		assertThat(setOps.members(key), isEqual(expected));
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
		assertEquals(Long.valueOf(2), setOps.remove(key, v1, v2, v4));
		assertThat(setOps.members(key), isEqual(Collections.singleton(v3)));
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
			assertThat(it.next(), anyOf(equalTo(v1), equalTo(v2), equalTo(v3)));
			count++;
		}
		it.close();
		assertThat(count, is(setOps.size(key)));
	}

	@Test // DATAREDIS-448
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

		assertThat(setOps.intersectAndStore(sourceKey1, sourceKey2, destinationKey), is(2L));
	}
}
