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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertThat;
import static org.springframework.data.redis.matcher.RedisTestMatchers.isEqual;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Integration test of {@link DefaultSetOperations}
 * 
 * @author Jennifer Hickey
 * 
 */
@RunWith(Parameterized.class)
public class DefaultSetOperationsTests<K,V> {
	
	private RedisTemplate<K,V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private SetOperations<K,V> setOps;

	public DefaultSetOperationsTests(RedisTemplate<K,V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}
	
	@Before
	public void setUp() {
		setOps = redisTemplate.opsForSet();
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
		assertTrue(Arrays.asList(new Object[] {v1, v2, v3}).containsAll(members));
	}

	@Test
	public void testRandomMembersWithDuplicates() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		setOps.add(setKey, v1);
		List<V> members = setOps.randomMembers(setKey, 2);
		assertEquals(Arrays.asList(new Object[] {v1, v1}), members);
	}

	@Test
	public void testRandomMembersNegative() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		try {
			setOps.randomMembers(keyFactory.instance(), -1);
			fail("IllegalArgumentException should be thrown");
		}catch(IllegalArgumentException e) {
		}
	}

	@Test
	public void testDistinctRandomMembersNegative() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		try {
			setOps.distinctRandomMembers(keyFactory.instance(), -2);
			fail("IllegalArgumentException should be thrown");
		}catch(IllegalArgumentException e) {
		}
	}

	@Test
	public void testMove() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		setOps.add(key1, v1);
		setOps.add(key1, v2);
		setOps.move(key1, v1, key2);
		assertThat(setOps.members(key1),
				isEqual(new HashSet<V>(Collections.singletonList(v2))));
		assertThat(setOps.members(key2),
				isEqual(new HashSet<V>(Collections.singletonList(v1))));
	}

	@Test
	public void testPop() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		setOps.add(key, v1);
		assertThat(setOps.pop(key), isEqual(v1));
		assertTrue(setOps.members(key).isEmpty());
	}

	@Test
	public void testRandomMember() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		setOps.add(key, v1);
		assertThat(setOps.randomMember(key), isEqual(v1));
	}
}
